use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::Path;

use geos::Geometry;

use osmpbfreader::objects::{Node, NodeId, OsmId, Tags, Way};
use osmpbfreader::{OsmObj, OsmPbfReader, StoreObjs};

use rusqlite::{Connection, DropBehavior, ToSql, NO_PARAMS};

const TAGS_TO_KEEP: &[&str] = &["addr:housenumber", "addr:street", "addr:unit", "addr:city", "addr:district", "addr:region", "addr:postcode"];

struct Address {
    lat: f64,
    lon: f64,
    number: Option<String>,
    street: Option<String>,
    unit: Option<String>,
    city: Option<String>,
    district: Option<String>,
    region: Option<String>,
    postcode: Option<String>,
}

impl Address {
    fn new(tags: &Tags, lat: f64, lon: f64) -> Address {
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
}

macro_rules! get_kind {
    ($obj:expr) => {
        if $obj.is_node() {
            &0
        } else {
            &1
        }
    }
}

struct DBNodes {
    conn: Connection,
    buffer: HashMap<OsmId, OsmObj>,
    buffer_size: usize,
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
        let mut tx = self.conn.transaction().expect("DBNodes::flush: transaction creation failed");
        tx.set_drop_behavior(DropBehavior::Ignore);

        {
            let mut stmt = tx.prepare("INSERT OR IGNORE INTO ids(id, obj, kind) VALUES (?1, ?2, ?3)").expect("DBNodes::flush: prepare failed");
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
        tx.commit().expect("DBNodes::flush: transaction commit failed");
    }

    fn get_from_id(&self, id: &OsmId) -> Cow<Node> {
        if let Some(obj) = self.buffer.get(id) {
            return match obj {
                OsmObj::Node(n) => Cow::Borrowed(n),
                _ => panic!("we're not supposed to have something else than a node in a way!"),
            };
        }
        let mut stmt = self.conn.prepare("SELECT obj FROM ids WHERE id=?1 AND kind=?2").expect("DB::get_from_id: prepare failed"
        );
        let mut iter = stmt.query(&[&id.inner_id() as &dyn ToSql, get_kind!(id)]).expect("DB::get_from_id: query_map failed");
        while let Some(row) = iter.next().expect("DBNodes::get_from_id: next failed") {
            let obj: Vec<u8> = row.get(0).expect("DBNodes::get_from_id: failed to get obj field");
            match bincode::deserialize(&obj).expect("DBNodes::for_each: serde conversion failed") {
                OsmObj::Node(n) => return Cow::Owned(n),
                _ => panic!("we're not supposed to have something else than a node in a way!"),
            }
        }
        panic!("node missing from a way!");
    }

    fn iter_objs<F: FnMut(&OsmObj, &[Cow<Node>])>(&self, mut f: F) {
        for (id, obj) in self.buffer.iter() {
            match obj {
                OsmObj::Way(w) => {
                    let nodes = w.nodes.iter().map(|n| self.get_from_id(&OsmId::Node(*n))).collect::<Vec<_>>();
                    f(obj, &nodes)
                }
                OsmObj::Node(n) => f(obj, &[]),
                _ => unreachable!(),
            }
        }
        let mut stmt = self.conn.prepare("SELECT obj FROM nodes").expect("failed");
        let person_iter = stmt.query_map(NO_PARAMS, |row| {
            let obj: Vec<u8> = row.get(0).expect("failed to get obj field");
            Ok(bincode::deserialize::<OsmObj>(&obj).expect("DBNodes::iter_objs: serde conversion failed"))
        }).expect("couldn't create iterator on query");
        for obj in person_iter {
            let obj = obj.expect("why is it still wrapped???");
            match obj {
                OsmObj::Way(ref w) => {
                    let nodes = w.nodes.iter().map(|n| self.get_from_id(&OsmId::Node(*n))).collect::<Vec<_>>();
                    f(&obj, &nodes)
                }
                OsmObj::Node(ref n) => f(&obj, &[]),
                _ => unreachable!(),
            }
        }
    }
}

impl StoreObjs for DBNodes {
    fn insert(&mut self, id: OsmId, mut obj: OsmObj) {
        match obj {
            OsmObj::Node(ref mut n) => {
                n.tags = (*n.tags).clone().into_iter().filter(|t| TAGS_TO_KEEP.iter().any(|x| *x == t.0.as_str())).collect();
            }
            OsmObj::Way(ref mut w) => {
                if w.nodes.is_empty() {
                    return;
                }
                w.tags = (*w.tags).clone().into_iter().filter(|t| t.0 == "addr:housenumber" || t.0 == "addr:street").collect();
                if w.tags.len() < 2 {
                    return;
                }
            }
            _ => return,
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
        let mut stmt = self.conn
            .prepare("SELECT id FROM nodes WHERE id=?1 AND kind=?2")
            .expect("DB::contains_key: prepare failed");
        let mut iter = stmt.query(&[&id.inner_id() as &dyn ToSql, get_kind!(id)])
            .expect("DB::contains_key: query_map failed");
        iter.next().expect("DB::contains_key: no row").is_some()
    }
}

impl Drop for DBNodes {
    fn drop(&mut self) {
        self.flush_buffer();
    }
}

pub struct DB {
    conn: Connection,
    buffer: Vec<Address>,
    db_buffer_size: usize,
}

impl DB {
    fn new(db_file: &str, db_buffer_size: usize, remove_db_data: bool) -> Result<DB, String> {
        if remove_db_data {
            let _ = fs::remove_file(db_file); // we ignore any potential error
        }
        let conn = Connection::open(db_file)
            .map_err(|e| format!("failed to open SQLITE connection: {}", e))?;

        if remove_db_data {
            conn.execute("DROP TABLE IF EXISTS addresses", NO_PARAMS)
                .expect("failed to drop addresses");
            conn.execute("DROP TABLE IF EXISTS addresses_errors", NO_PARAMS)
                .expect("failed to drop errors");
        }
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS addresses(
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                number TEXT,
                street TEXT NOT NULL,
                unit TEXT,
                city TEXT,
                district TEXT,
                region TEXT,
                postcode TEXT,
                PRIMARY KEY (lat, lon, number, street, city)
            )"#,
            NO_PARAMS,
        )
        .map_err(|e| format!("failed to create table: {}", e))?;
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS addresses_errors(
                lat REAL,
                lon REAL,
                number TEXT,
                street TEXT,
                unit TEXT,
                city TEXT,
                district TEXT,
                region TEXT,
                postcode TEXT,
                kind TEXT
            )"#,
            NO_PARAMS,
        )
        .map_err(|e| format!("failed to create error table: {}", e))?;
        Ok(DB {
            conn,
            buffer: Vec::with_capacity(db_buffer_size),
            db_buffer_size,
        })
    }

    fn flush_buffer(&mut self) {
        let mut tx = self.conn.transaction().expect("failed to open transaction");
        tx.set_drop_behavior(DropBehavior::Ignore);

        let mut errors = {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO addresses(
                    lat,
                    lon,
                    number,
                    street,
                    unit,
                    city,
                    district,
                    region,
                    postcode
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                )
                .expect("failed to prepare statement");

            self.buffer
                .drain(..)
                .filter_map(|obj| {
                    if let Err(e) = stmt.execute(&[
                        &obj.lat as &dyn ToSql,
                        &obj.lon,
                        &obj.number,
                        &obj.street,
                        &obj.unit,
                        &obj.city,
                        &obj.district,
                        &obj.region,
                        &obj.postcode,
                    ]) {
                        Some((obj, e.to_string()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        if !errors.is_empty() {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO addresses_errors(
                    lat,
                    lon,
                    number,
                    street,
                    unit,
                    city,
                    district,
                    region,
                    postcode,
                    kind
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                )
                .expect("failed to prepare error statement");

            for (obj, err) in errors.drain(..) {
                stmt.execute(&[
                    &obj.lat as &dyn ToSql,
                    &obj.lon,
                    &obj.number,
                    &obj.street,
                    &obj.unit,
                    &obj.city,
                    &obj.district,
                    &obj.region,
                    &obj.postcode,
                    &err,
                ])
                .expect("failed to insert into errors");
            }
        }

        tx.commit().expect("commit failed");
    }

    fn insert(&mut self, addr: Address) {
        self.buffer.push(addr);
        if self.buffer.len() >= self.db_buffer_size {
            self.flush_buffer();
        }
    }

    pub fn get_nb_cities(&self) -> i64 {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM addresses GROUP BY city")
            .expect("failed to prepare");
        let mut iter = stmt
            .query_map(NO_PARAMS, |row| Ok(row.get(0)?))
            .expect("query_map failed");
        iter.next().expect("no count???").expect("failed")
    }

    pub fn get_nb_addresses(&self) -> i64 {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM addresses")
            .expect("failed to prepare");
        let mut iter = stmt
            .query_map(NO_PARAMS, |row| Ok(row.get(0)?))
            .expect("query_map failed");
        iter.next().expect("no count???").expect("failed")
    }

    pub fn get_nb_errors(&self) -> i64 {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM addresses_errors")
            .expect("failed to prepare");
        let mut iter = stmt
            .query_map(NO_PARAMS, |row| Ok(row.get(0)?))
            .expect("query_map failed");
        iter.next().expect("no count???").expect("failed")
    }

    pub fn get_nb_by_errors_kind(&self) -> Vec<(String, i64)> {
        let mut stmt = self
            .conn
            .prepare("SELECT kind, COUNT(*) FROM addresses_errors GROUP BY kind")
            .expect("failed to prepare");
        stmt.query_map(NO_PARAMS, |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("query_map failed")
            .map(|x| x.expect("failed"))
            .collect()
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        self.flush_buffer();
    }
}

fn get_nodes<P: AsRef<Path>>(pbf_file: P) -> DBNodes {
    let mut reader = OsmPbfReader::new(File::open(&pbf_file).expect(&format!(
        "Failed to open file `{}`",
        pbf_file.as_ref().display()
    )));

    let mut db_nodes = DBNodes::new("nodes.db", 10000).expect("failed to create DBNodes");
    reader.get_objs_and_deps_store(|obj| {
        match obj {
            OsmObj::Node(o) => {
                o.tags.iter().any(|x| x.0 == "addr:housenumber") &&
                o.tags.iter().any(|x| x.0 == "addr:street")
            }
            OsmObj::Way(w) => {
                w.tags.iter().any(|x| x.0 == "addr:housenumber") &&
                w.tags.iter().any(|x| x.0 == "addr:street")
            }
            _ => false,
        }
    }, &mut db_nodes);

    db_nodes.flush_buffer();
    println!("Got {} potential addresses!", db_nodes.get_nb_entries());
    db_nodes
}

pub fn import_addresses<P: AsRef<Path>>(
    db_file_name: &str,
    pbf_file: P,
    remove_db_data: bool,
) -> DB {
    let db_nodes = get_nodes(pbf_file);
    // for obj in reader.iter().filter_map(|o| match o {
    //     Ok(OsmObj::Node(o)) => {
    //         map.insert(o.id, (o.decimicro_lat, o.decimicro_lon));
    //         if o.tags.iter().filter(|x| x.0.contains("addr:")).count() > 1 {
    //             Some(o)
    //         } else {
    //             None
    //         }
    //     }
    //     _ => None,
    // }) {
    //     // db.insert(Address::new(&obj.tags, obj.lat(), obj.lon()));
    // }

    // let objs = reader
    //     .get_objs_and_deps(|o| match o {
    //         // OsmObj::Node(n) => n.tags.iter().filter(|x| x.0.contains("addr:")).count() > 1,
    //         OsmObj::Way(w) => {
    //             !w.nodes.is_empty() &&
    //             w.tags.iter().any(|x| x.0 == "addr:housenumber")
    //                 && w.tags.iter().any(|x| x.0 == "addr:street")
    //         }
    //         _ => false,
    //     })
    //     .expect("failed to run get_objs_and_deps");
    // for (_, obj) in &objs {
    //     match obj {
    //         OsmObj::Node(n) => {
    //             db.insert(Address::new(&n.tags, n.lat(), n.lon()));
    //         }
    //         OsmObj::Way(w) => {
    //             let nodes: Vec<&Node> = w
    //                 .nodes
    //                 .iter()
    //                 .filter_map(|id| match objs.get(&OsmId::Node(*id)) {
    //                     Some(OsmObj::Node(n)) => Some(n),
    //                     _ => None,
    //                 })
    //                 .collect();
    //             if nodes.is_empty() {
    //                 continue;
    //             } else if nodes.len() == 1 {
    //                 db.insert(Address::new(&w.tags, nodes[0].lat(), nodes[0].lon()));
    //                 continue;
    //             }
    //             let polygon = format!(
    //                 "POLYGON(({}))",
    //                 nodes
    //                     .into_iter()
    //                     .map(|n| format!("{} {}", n.lon(), n.lat()))
    //                     .collect::<Vec<_>>()
    //                     .join(",")
    //             );
    //             if let Ok(geom) = Geometry::new_from_wkt(&polygon).and_then(|g| g.get_centroid()) {
    //                 let (lon, lat) = match (geom.get_x(), geom.get_y()) {
    //                     (Ok(lon), Ok(lat)) => (lon, lat),
    //                     _ => continue,
    //                 };
    //                 db.insert(Address::new(&w.tags, lat, lon));
    //             } else {
    //                 continue;
    //             }
    //         }
    //         _ => {}
    //     }
    // }
    // println!("END SIZE: {:?}", map.len());

    let mut db = DB::new(db_file_name, 1000, remove_db_data).expect("Failed to create DB");
    db_nodes.iter_objs(|obj, nodes| {
        match obj {
            OsmObj::Node(node) => db.insert(Address::new(&node.tags, node.lat(), node.lon())),
            OsmObj::Way(way) => {
                if nodes.len() == 1 {
                    db.insert(Address::new(&way.tags, nodes[0].lat(), nodes[0].lon()));
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
                    db.insert(Address::new(&way.tags, lat, lon));
                } else {
                    return;
                }
            }
            _ => unreachable!(),
        }
    });
    db
}
