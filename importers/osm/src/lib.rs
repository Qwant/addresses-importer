use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::Path;

use geos::Geometry;

use osmpbfreader::objects::{OsmId, RelationId, Tags, WayId};
use osmpbfreader::{OsmObj, OsmPbfReader, StoreObjs};

use rusqlite::{Connection, DropBehavior, ToSql, NO_PARAMS};

use tools::{Address, CompatibleDB};

const TAGS_TO_KEEP: &[&str] = &[
    "addr:housenumber",
    "addr:street",
    "addr:unit",
    "addr:city",
    "addr:district",
    "addr:region",
    "addr:postcode",
];

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

    fn get_nb_entries(&self) -> i64 {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM nodes")
            .expect("failed to prepare");
        let mut iter = stmt
            .query_map(NO_PARAMS, |row| Ok(row.get(0)?))
            .expect("query_map failed");
        iter.next().expect("no count???").expect("failed")
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
                        eprintln!("DBNodes::flush: failed to convert to json: {}", e);
                        continue;
                    }
                };
                let kind = get_kind!(obj);
                if let Err(e) = stmt.execute(&[&id.inner_id() as &dyn ToSql, &ser_obj, kind]) {
                    eprintln!("DBNodes::flush: insert failed: {}", e);
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
        while let Some(row) = iter.next().expect("DBNodes::get_from_id: next failed") {
            let obj: Vec<u8> = row
                .get(0)
                .expect("DBNodes::get_from_id: failed to get obj field");
            return Some(Cow::Owned(
                bincode::deserialize(&obj).expect("DBNodes::for_each: serde conversion failed"),
            ));
        }
        None
    }

    fn iter_objs<F: FnMut(&OsmObj, &[Cow<OsmObj>])>(&self, mut f: F) {
        for (_, obj) in self.buffer.iter() {
            match obj {
                OsmObj::Way(w) => {
                    let nodes = w
                        .nodes
                        .iter()
                        .filter_map(|n| self.get_from_id(&OsmId::Node(*n)))
                        .collect::<Vec<_>>();
                    f(obj, &nodes)
                }
                OsmObj::Relation(r) => {
                    let nodes = r
                        .refs
                        .iter()
                        .filter_map(|r| {
                            if r.member.is_node() {
                                self.get_from_id(&r.member)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    f(obj, &nodes)
                }
                OsmObj::Node(_) => f(obj, &[]),
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
            match obj {
                OsmObj::Way(ref w) => {
                    let nodes = w
                        .nodes
                        .iter()
                        .filter_map(|n| self.get_from_id(&OsmId::Node(*n)))
                        .collect::<Vec<_>>();
                    f(&obj, &nodes)
                }
                OsmObj::Node(_) => f(&obj, &[]),
                OsmObj::Relation(ref r) => {
                    let nodes = r
                        .refs
                        .iter()
                        .filter_map(|n| {
                            if n.member.is_node() {
                                self.get_from_id(&n.member)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    f(&obj, &nodes)
                }
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
                if w.tags.len() < 2 {
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

// This functions gets all ways present in relations.
fn get_ways<P: AsRef<Path>>(pbf_file: P) -> (HashSet<WayId>, HashSet<RelationId>) {
    let mut db_nodes = DBNodes::new("tmp.db", 10000).expect("failed to create DBNodes");
    let mut ways = HashSet::with_capacity(10000);
    let mut sub_relations = HashSet::with_capacity(100);
    let mut reader = OsmPbfReader::new(File::open(&pbf_file).expect(&format!(
        "Failed to open file `{}`",
        pbf_file.as_ref().display()
    )));
    reader
        .get_objs_and_deps_store(
            |obj| match obj {
                OsmObj::Relation(r) => {
                    if !r
                        .refs
                        .iter()
                        .filter(|x| x.member.is_way() || x.member.is_relation())
                        .count()
                        > 0
                        && r.tags
                            .iter()
                            .any(|x| x.0 == "type" && x.1 == "associatedStreet")
                        && r.tags.iter().any(|x| x.0 == "name")
                    {
                        for entry in r.refs.iter() {
                            match entry.member {
                                OsmId::Way(id) => {
                                    ways.insert(id);
                                }
                                OsmId::Relation(id) => {
                                    sub_relations.insert(id);
                                }
                                _ => {}
                            }
                        }
                        true
                    } else {
                        sub_relations.contains(&r.id)
                    }
                }
                _ => false,
            },
            &mut db_nodes,
        )
        .expect("get_ways: get_objs_and_deps_store failed");
    (ways, sub_relations)
}

// This functions gets everything else alongside the relations we already have that might be useful.
fn get_nodes<P: AsRef<Path>>(
    pbf_file: P,
    ways: HashSet<WayId>,
    relations: HashSet<RelationId>,
) -> DBNodes {
    let mut db_nodes = DBNodes::new("nodes.db", 1000).expect("failed to create DBNodes");
    let mut reader = OsmPbfReader::new(File::open(&pbf_file).expect(&format!(
        "Failed to open file `{}`",
        pbf_file.as_ref().display()
    )));
    reader
        .get_objs_and_deps_store(
            |obj| match obj {
                OsmObj::Node(o) => {
                    o.tags.iter().any(|x| x.0 == "addr:housenumber")
                        && o.tags.iter().any(|x| x.0 == "addr:street")
                }
                OsmObj::Way(w) => {
                    (!w.nodes.is_empty()
                        && w.tags.iter().any(|x| x.0 == "addr:housenumber")
                        && w.tags.iter().any(|x| x.0 == "addr:street"))
                        || ways.contains(&w.id)
                }
                OsmObj::Relation(r) => {
                    (!r.refs.is_empty()
                        && r.tags
                            .iter()
                            .any(|x| x.0 == "type" && x.1 == "associatedStreet")
                        && r.tags.iter().any(|x| x.0 == "name"))
                        || relations.contains(&r.id)
                }
            },
            &mut db_nodes,
        )
        .expect("get_nodes: get_objs_and_deps_store failed");

    db_nodes.flush_buffer();
    println!("Got {} potential addresses!", db_nodes.get_nb_entries());
    db_nodes
}

fn get_time() -> String {
    let now = time::Time::now();
    format!("{:02}:{:02}:{:02}", now.hour(), now.minute(), now.second())
}

pub fn import_addresses<P: AsRef<Path>, T: CompatibleDB>(pbf_file: P, db: &mut T) {
    println!("[{}] Getting ways...", get_time());
    let (ways, relations) = get_ways(&pbf_file);
    println!(
        "[{}] Got {} ways and {} relations",
        get_time(),
        ways.len(),
        relations.len()
    );
    println!("[{}] Getting nodes...", get_time());
    let db_nodes = get_nodes(pbf_file, ways, relations);
    println!("[{}] Got {} nodes", get_time(), db_nodes.count());
    println!("[{}] Filling address DB...", get_time());
    db_nodes.iter_objs(|obj, sub_objs| {
        match obj {
            OsmObj::Node(node) => db.insert(new_address(&node.tags, node.lat(), node.lon())),
            OsmObj::Way(way) => {
                let nodes = sub_objs
                    .iter()
                    .map(|x| match &**x {
                        OsmObj::Node(n) => n,
                        _ => panic!("nothing else than nodes should be in a way!"),
                    })
                    .collect::<Vec<_>>();
                if nodes.len() == 1 {
                    db.insert(new_address(&way.tags, nodes[0].lat(), nodes[0].lon()));
                    return;
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
                    let (lon, lat) = match (geom.get_x(), geom.get_y()) {
                        (Ok(lon), Ok(lat)) => (lon, lat),
                        _ => return,
                    };
                    db.insert(new_address(&way.tags, lat, lon));
                } else {
                    return;
                }
            }
            OsmObj::Relation(r) => {
                let addr_name = match r.tags.iter().find(|t| t.0 == "name").map(|t| t.1) {
                    Some(addr) => addr,
                    None => unreachable!(),
                };
                for sub_obj in sub_objs {
                    let node = match &**sub_obj {
                        OsmObj::Node(n) => n,
                        // We currently don't handle ways and relations inside the relations...
                        _ => continue,
                    };
                    if !node.tags.iter().any(|t| t.0 == "addr:housenumber") {
                        return;
                    }
                    let mut addr = new_address(&node.tags, node.lat(), node.lon());
                    addr.street = Some(addr_name.clone());
                    db.insert(addr);
                }
            }
        }
    });
    println!("[{}] Added {} addresses", get_time(), db.get_nb_addresses());
}
