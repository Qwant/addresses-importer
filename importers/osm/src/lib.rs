//! Reading a **PBF** file is actually complicated: elements refer to each others using IDs, forcing
//! the parser to go back and forth in the file (unless you have a lot of available RAM!).
//!
//! So for this, we run it in 2 passes:
//!  1. We store all matching objects (filter rules explained below) in a temporary database.
//!  2. We iter through the stored objects to put them in the provided `db`.
//!
//! In here, we filter objects as follow:
//!  * If it's a **node**, we look if it has the tags `addr:housenumber` and `addr:street`. If so,
//!    we consider it as an address and add it.
//!  * If it's a **way**, it needs to contain the tags `addr:housenumber` and `addr:street` and also
//!    at least one node.
//!  * If it's a **relation**, it needs to contains the tag `name` and the tag `type` with the value
//!    `associatedStreet` and at least one sub-reference. Then we read the sub-references an apply
//!    the same rules depending if's a **node** or a **way**. We currently ignore the sub-references
//!    if they are **relation**s.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::Path;

use geos::Geometry;

use osmpbfreader::objects::{OsmId, Tags};
use osmpbfreader::{OsmObj, OsmPbfReader, StoreObjs};

use rusqlite::{Connection, DropBehavior, ToSql, NO_PARAMS};

use tools::{teprint, teprintln, tprintln, Address, CompatibleDB};

/// Used to make the stored elements in the first lighter by removing all the unused tags.
const TAGS_TO_KEEP: &[&str] = &[
    "addr:housenumber",
    "addr:street",
    "addr:unit",
    "addr:city",
    "addr:district",
    "addr:region",
    "addr:postcode",
];

/// We need to know what kind the element is when reading the database in order to deserialize it.
macro_rules! get_kind {
    ($obj:expr) => {
        if $obj.is_node() {
            &0
        } else if $obj.is_way() {
            &1
        } else {
            &2
        }
    };
}

/// Convert an element's tags into an address.
///
/// In here, we look at the following tags:
///  * `addr:housenumber`
///  * `addr:street`
///  * `addr:unit`
///  * `addr:city`
///  * `addr:district`
///  * `addr:region`
///  * `addr:postcode`
///
/// This might evolve in the future considering that some countries use different tags to store the
/// same information.
fn new_address(tags: &Tags, lat: f64, lon: f64) -> Address {
    let mut addr = Address {
        lat,
        lon,
        number: None,
        street: None,
        unit: None,
        city: None,
        district: None,
        region: None,
        postcode: None,
    };

    for (tag, value) in tags.iter() {
        match tag.as_str() {
            "addr:housenumber" => {
                addr.number = Some(value.to_owned());
            }
            "addr:street" => {
                addr.street = Some(value.to_owned());
            }
            "addr:unit" => {
                addr.unit = Some(value.to_owned());
            }
            "addr:city" => {
                addr.city = Some(value.to_owned());
            }
            "addr:district" => {
                addr.district = Some(value.to_owned());
            }
            "addr:region" => {
                addr.region = Some(value.to_owned());
            }
            "addr:postcode" => {
                addr.postcode = Some(value.to_owned());
            }
            _ => {}
        }
    }
    addr
}

/// Type used to store elements in the "first pass".
#[derive(Debug)]
enum StoredObj<'a> {
    Relation(Cow<'a, OsmObj>, Vec<StoredObj<'a>>),
    Way(Cow<'a, OsmObj>, Vec<Cow<'a, OsmObj>>),
    Node(Cow<'a, OsmObj>),
}

/// Database wrapper used to store the potential addresses. They are stored using the [`StoredObj`]
/// enum. A buffer is used in order to not store everything on the disk.
// TODO: maybe we should remove this buffer and directly use the SQLite cache?
struct DBNodes {
    conn: Connection,
    buffer: HashMap<OsmId, OsmObj>,
    buffer_size: usize,
    db_file: String,
}

impl DBNodes {
    fn new(db_file: &str, buffer_size: usize) -> Result<DBNodes, String> {
        let _ = fs::remove_file(db_file); // we ignore any potential error
        let conn = Connection::open(db_file)
            .map_err(|e| format!("failed to open SQLITE connection: {}", e))?;
        conn.execute("DROP TABLE IF EXISTS nodes", NO_PARAMS)
            .expect("failed to drop nodes");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nodes (
                id   INTEGER NOT NULL,
                obj  BLOB NOT NULL,
                kind INTEGER NOT NULL,
                UNIQUE(id, kind)
             )",
            NO_PARAMS,
        )
        .map_err(|e| format!("failed to create table: {}", e))?;
        Ok(DBNodes {
            conn,
            buffer: HashMap::with_capacity(buffer_size),
            buffer_size,
            db_file: db_file.to_owned(),
        })
    }

    fn flush_buffer(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let mut tx = self
            .conn
            .transaction()
            .expect("DBNodes::flush: transaction creation failed");
        tx.set_drop_behavior(DropBehavior::Ignore);

        {
            let mut stmt = tx
                .prepare("INSERT OR IGNORE INTO nodes(id, obj, kind) VALUES (?1, ?2, ?3)")
                .expect("DBNodes::flush: prepare failed");
            for (id, obj) in self.buffer.drain() {
                let ser_obj = match bincode::serialize(&obj) {
                    Ok(s) => s,
                    Err(e) => {
                        teprintln!("[OSM] DBNodes::flush: failed to convert to json: {}", e);
                        continue;
                    }
                };
                let kind = get_kind!(obj);
                if let Err(e) = stmt.execute(&[&id.inner_id() as &dyn ToSql, &ser_obj, kind]) {
                    teprintln!("[OSM] DBNodes::flush: insert failed: {}", e);
                }
            }
        }
        tx.commit()
            .expect("DBNodes::flush: transaction commit failed");
    }

    fn get_from_id(&self, id: &OsmId) -> Option<Cow<OsmObj>> {
        if let Some(obj) = self.buffer.get(id) {
            return Some(Cow::Borrowed(obj));
        }
        let mut stmt = self
            .conn
            .prepare("SELECT obj FROM nodes WHERE id=?1 AND kind=?2")
            .expect("DB::get_from_id: prepare failed");
        let mut iter = stmt
            .query(&[&id.inner_id() as &dyn ToSql, get_kind!(id)])
            .expect("DB::get_from_id: query_map failed");
        if let Some(row) = iter.next().expect("DBNodes::get_from_id: next failed") {
            let obj: Vec<u8> = row
                .get(0)
                .expect("DBNodes::get_from_id: failed to get obj field");
            return Some(Cow::Owned(
                bincode::deserialize(&obj).expect("DBNodes::for_each: serde conversion failed"),
            ));
        }
        None
    }

    fn get_way<'a>(&'a self, way: Cow<'a, OsmObj>) -> StoredObj<'a> {
        let nodes = match &*way {
            OsmObj::Way(w) => w
                .nodes
                .iter()
                .filter_map(|n| self.get_from_id(&OsmId::Node(*n)))
                .collect::<Vec<_>>(),
            _ => panic!("only way can be used in get_way"),
        };
        StoredObj::Way(way, nodes)
    }

    fn get_relation<'a>(&'a self, rel: Cow<'a, OsmObj>) -> StoredObj<'a> {
        let nodes = match &*rel {
            OsmObj::Relation(r) => r
                .refs
                .iter()
                .filter_map(|n| {
                    let elem = self.get_from_id(&n.member)?;
                    if elem.is_way() {
                        Some(self.get_way(elem))
                    } else if elem.is_relation() {
                        Some(self.get_relation(elem))
                    } else {
                        Some(StoredObj::Node(elem))
                    }
                })
                .collect::<Vec<_>>(),
            _ => panic!("only relations can be used in get_relation"),
        };
        StoredObj::Relation(rel, nodes)
    }

    fn iter_objs<'a, F: FnMut(StoredObj<'a>)>(&'a self, mut f: F) {
        for (_, obj) in self.buffer.iter() {
            if obj.is_way() {
                f(self.get_way(Cow::Borrowed(obj)))
            } else if obj.is_relation() {
                f(self.get_relation(Cow::Borrowed(obj)))
            } else if obj.tags().iter().any(|t| t.0 == "addr:housenumber")
                && obj.tags().iter().any(|t| t.0 == "addr:street")
            {
                f(StoredObj::Node(Cow::Borrowed(obj)))
            }
        }
        let mut stmt = self.conn.prepare("SELECT obj FROM nodes").expect("failed");
        let person_iter = stmt
            .query_map(NO_PARAMS, |row| {
                let obj: Vec<u8> = row.get(0).expect("failed to get obj field");
                Ok(bincode::deserialize::<OsmObj>(&obj)
                    .expect("DBNodes::iter_objs: serde conversion failed"))
            })
            .expect("couldn't create iterator on query");
        for obj in person_iter {
            let obj = obj.expect("why is it still wrapped???");
            if obj.is_way() {
                f(self.get_way(Cow::Owned(obj)))
            } else if obj.is_relation() {
                f(self.get_relation(Cow::Owned(obj)))
            } else if obj.tags().iter().any(|t| t.0 == "addr:housenumber")
                && obj.tags().iter().any(|t| t.0 == "addr:street")
            {
                f(StoredObj::Node(Cow::Owned(obj)))
            }
        }
    }

    fn count(&self) -> i64 {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM nodes")
            .expect("failed to prepare");
        let mut iter = stmt
            .query_map(NO_PARAMS, |row| Ok(row.get(0)?))
            .expect("query_map failed");
        iter.next().expect("no count???").expect("failed")
    }
}

impl StoreObjs for DBNodes {
    fn insert(&mut self, id: OsmId, mut obj: OsmObj) {
        match obj {
            OsmObj::Node(ref mut n) => {
                n.tags
                    .retain(|k, _| TAGS_TO_KEEP.iter().any(|x| *x == k.as_str()));
            }
            OsmObj::Way(ref mut w) => {
                w.tags
                    .retain(|k, _| k == "addr:housenumber" || k == "addr:street");
                if w.tags.is_empty() {
                    // We're supposed to have at least the housenumber (in case we're in a
                    // relation) or the street (in case we're a street with housenumbers).
                    return;
                }
            }
            OsmObj::Relation(ref mut r) => {
                if !r.tags.iter().any(|x| x.0 == "name") {
                    return;
                }
                r.tags.retain(|k, _| k == "name");
            }
        }
        self.buffer.insert(id, obj);
        if self.buffer.len() >= self.buffer_size {
            self.flush_buffer();
        }
    }

    fn contains_key(&self, id: &OsmId) -> bool {
        if self.buffer.contains_key(id) {
            return true;
        }
        let mut stmt = self
            .conn
            .prepare("SELECT id FROM nodes WHERE id=?1 AND kind=?2")
            .expect("DB::contains_key: prepare failed");
        let mut iter = stmt
            .query(&[&id.inner_id() as &dyn ToSql, get_kind!(id)])
            .expect("DB::contains_key: query_map failed");
        iter.next().expect("DB::contains_key: no row").is_some()
    }
}

impl Drop for DBNodes {
    fn drop(&mut self) {
        self.conn.flush_prepared_statement_cache();
        let _ = fs::remove_file(&self.db_file); // we ignore any potential error bis
    }
}

/// Used in the "first pass" to generate the database fulfilled with all the potential addresses
/// present in the PBF file.
///
/// To learn more about the filtering rules, please refer to the crate level documentation.
fn get_nodes<P: AsRef<Path>>(pbf_file: P) -> DBNodes {
    let mut db_nodes = DBNodes::new("nodes.db", 1000).expect("failed to create DBNodes");
    {
        let mut reader =
            OsmPbfReader::new(File::open(&pbf_file).unwrap_or_else(|err| {
                panic!("Failed to open file {:?}: {}", pbf_file.as_ref(), err)
            }));
        reader
            .get_objs_and_deps_store(
                |obj| match obj {
                    OsmObj::Node(o) => {
                        o.tags.iter().any(|x| x.0 == "addr:housenumber")
                            && o.tags.iter().any(|x| x.0 == "addr:street")
                    }
                    OsmObj::Way(w) => {
                        !w.nodes.is_empty()
                            && w.tags.iter().any(|x| x.0 == "addr:housenumber")
                            && w.tags.iter().any(|x| x.0 == "addr:street")
                    }
                    OsmObj::Relation(r) => {
                        !r.refs.is_empty()
                            && r.tags
                                .iter()
                                .any(|x| x.0 == "type" && x.1 == "associatedStreet")
                            && r.tags.iter().any(|x| x.0 == "name")
                    }
                },
                &mut db_nodes,
            )
            .expect("get_nodes: get_objs_and_deps_store failed");
    }
    db_nodes.flush_buffer();
    db_nodes
}

/// Function to generate a position for a **way**. If the **way** is only composed of one **node**,
/// it'll return the latitude and longitude of this **node**. If there is more than one, it'll first
/// create a polygon and then get its centroid's latitude and longitude.
///
/// In case of error when generating the polygon, it'll return `None`.
fn get_way_lat_lon(sub_objs: &[Cow<OsmObj>]) -> Option<(f64, f64)> {
    let nodes = sub_objs
        .iter()
        .map(|x| match &**x {
            OsmObj::Node(n) => n,
            _ => panic!("nothing else than nodes should be in a way!"),
        })
        .collect::<Vec<_>>();
    if nodes.len() == 1 {
        return Some((nodes[0].lat(), nodes[0].lon()));
    }
    let polygon = format!(
        "POLYGON(({}))",
        nodes
            .into_iter()
            .map(|n| format!("{} {}", n.lon(), n.lat()))
            .collect::<Vec<_>>()
            .join(",")
    );
    if let Ok(geom) = Geometry::new_from_wkt(&polygon).and_then(|g| g.get_centroid()) {
        if let (Ok(lon), Ok(lat)) = (geom.get_x(), geom.get_y()) {
            return Some((lat, lon));
        };
    }
    None
}

/// Function used in the "first pass" by the [`iter_nodes`] function.
///
/// The goal here is to filter out all the elements that don't seem to be addresses and store the
/// others into the provided `db` argument.
///
/// The conditions are explained at the crate level.
fn handle_obj<T: CompatibleDB>(obj: StoredObj, db: &mut T) {
    match obj {
        StoredObj::Node(n) => match &*n {
            OsmObj::Node(n) => db.insert(new_address(&n.tags, n.lat(), n.lon())),
            _ => unreachable!(),
        },
        StoredObj::Way(way, nodes) => {
            if let Some((lat, lon)) = get_way_lat_lon(&nodes) {
                db.insert(new_address(&way.tags(), lat, lon));
            }
        }
        StoredObj::Relation(r, objs) => {
            let addr_name = match r.tags().iter().find(|t| t.0 == "name").map(|t| t.1) {
                Some(addr) => addr,
                None => unreachable!(),
            };
            for sub_obj in objs {
                match sub_obj {
                    StoredObj::Node(n) if n.tags().iter().any(|t| t.0 == "addr:housenumber") => {
                        match &*n {
                            OsmObj::Node(n) => {
                                let mut addr = new_address(&n.tags, n.lat(), n.lon());
                                addr.street = Some(addr_name.clone());
                                db.insert(addr);
                            }
                            _ => unreachable!(),
                        }
                    }
                    StoredObj::Way(w, nodes)
                        if w.tags().iter().any(|t| t.0 == "addr:housenumber") =>
                    {
                        if let Some((lat, lon)) = get_way_lat_lon(&nodes) {
                            let mut addr = new_address(&w.tags(), lat, lon);
                            addr.street = Some(addr_name.clone());
                            db.insert(addr);
                        }
                    }
                    _ => {} // currently not handling relations in relations
                }
            }
        }
    }
}

/// This is the "first pass" function. It'll iterate through all objects of "interest" and store
/// them in the provided `db`. Take a look at the crate documentation for more details (notably for
/// how the filtering works).
fn iter_nodes<T: CompatibleDB>(db_nodes: DBNodes, db: &mut T) {
    db_nodes.iter_objs(|obj| handle_obj(obj, db));
}

/// The entry point of the **OpenStreetMap** importer.
///
/// * The `pbf_file` argument is the location the file containing all the **OpenStreetMap** data.
/// * The `db` argument is the mutable database wrapper implementing the `CompatibleDB` trait where
///   the data will be stored.
///
/// Example:
///
/// ```no_run
/// use tools::DB;
/// use osm::import_addresses;
///
/// let mut db = DB::new("addresses.db", 10000, true).expect("failed to create DB");
/// import_addresses("some_file.pbf", &mut db);
/// ```
pub fn import_addresses<P: AsRef<Path>, T: CompatibleDB>(pbf_file: P, db: &mut T) {
    let count_before = db.get_nb_addresses();

    teprint!("[OSM] Getting nodes ...\r");
    let db_nodes = get_nodes(pbf_file);
    teprintln!("[OSM] Getting nodes ... {} nodes", db_nodes.count());

    iter_nodes(db_nodes, db);

    let count_after = db.get_nb_addresses();
    tprintln!(
        "[OSM] Added {} addresses (total: {})",
        count_after - count_before,
        count_after
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use tools::*;

    #[test]
    fn check_relations() {
        let pbf_file = "test-files/relations_ways.pbf";
        let db_file = "check_relations.db";

        let mut db = DB::new(&db_file, 0, true).expect("Failed to initialize DB");
        let db_nodes = get_nodes(&pbf_file);
        assert_eq!(db_nodes.count(), 1406);
        iter_nodes(db_nodes, &mut db);
        assert_eq!(db.get_nb_addresses(), 361);
        let addr = db.get_address(2, "Place de la Forêt de Cruye");
        assert_eq!(addr.len(), 1);
        let _ = fs::remove_file(db_file); // we ignore any potential error
    }
}
