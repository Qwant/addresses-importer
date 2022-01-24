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

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use geos::Geometry;

use osmpbfreader::objects::{OsmId, Tags};
use osmpbfreader::{Node, OsmObj, OsmPbfReader, Relation, Way};

use tools::{teprint, tprintln, Address, CompatibleDB};

/// Size of the read buffer put on top of the input PBF file
const PBF_BUFFER_SIZE: usize = 1024 * 1024; // 1MB

const MAX_VALID_HOUSENUMBER_LENGTH: usize = 8;

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
    Relation(&'a OsmObj, Vec<StoredObj<'a>>),
    Way(&'a OsmObj, Vec<&'a OsmObj>),
    Node(&'a OsmObj),
}

fn build_way<'a>(obj: &'a OsmObj, all_objs: &'a BTreeMap<OsmId, OsmObj>) -> StoredObj<'a> {
    let nodes = match obj {
        OsmObj::Way(w) => w
            .nodes
            .iter()
            .filter_map(|n| all_objs.get(&OsmId::Node(*n)))
            .collect::<Vec<_>>(),
        _ => panic!("only way can be used in get_way"),
    };

    StoredObj::Way(obj, nodes)
}

fn build_relation<'a>(obj: &'a OsmObj, all_objs: &'a BTreeMap<OsmId, OsmObj>) -> StoredObj<'a> {
    let nodes = match obj {
        OsmObj::Relation(r) => r
            .refs
            .iter()
            .filter_map(|n| {
                let elem = all_objs.get(&n.member)?;

                if elem.is_way() {
                    Some(build_way(elem, all_objs))
                } else if elem.is_relation() {
                    Some(build_relation(elem, all_objs))
                } else {
                    Some(StoredObj::Node(elem))
                }
            })
            .collect::<Vec<_>>(),
        _ => panic!("only relations can be used in get_relation"),
    };

    StoredObj::Relation(obj, nodes)
}

/// Used in the "first pass" to generate the database fulfilled with all the potential addresses
/// present in the PBF file.
///
/// To learn more about the filtering rules, please refer to the crate level documentation.
fn get_nodes<T: CompatibleDB>(pbf_file: &Path, db: &mut T) {
    // Raw filter over different OSM types, they are similar except for
    // relations that would only contain an associated street.
    let node_filter = |n: &Node| {
        n.tags.iter().any(is_valid_housenumber_tag) && n.tags.iter().any(|x| x.0 == "addr:street")
    };

    let way_filter = |w: &Way| {
        !w.nodes.is_empty()
            && w.tags.iter().any(is_valid_housenumber_tag)
            && w.tags.iter().any(|x| x.0 == "addr:street")
    };

    let rel_filter = |r: &Relation| {
        !r.refs.is_empty()
            && r.tags
                .iter()
                .any(|x| x.0 == "type" && x.1 == "associatedStreet")
            && r.tags.iter().any(|x| x.0 == "name")
    };

    // Init reader
    let file = BufReader::with_capacity(
        PBF_BUFFER_SIZE,
        File::open(&pbf_file)
            .unwrap_or_else(|err| panic!("Failed to open file {:?}: {}", pbf_file, err)),
    );

    let mut reader = OsmPbfReader::new(file);

    // First, makes a pass on nodes only
    teprint!("[OSM] Fetching nodes ...\r");

    for obj in reader.par_iter() {
        let obj = obj.expect("could not read pbf");

        if obj.is_node() && node_filter(obj.node().unwrap()) {
            handle_obj(StoredObj::Node(&obj), db)
        }
    }

    // Then, makes a pass on ways and relations, which requires to store the hierarchy in memory.
    teprint!("[OSM] Fetching ways and relations ...\r");

    let osm_objs = reader
        .get_objs_and_deps(|obj| match obj {
            OsmObj::Node(_) => false,
            OsmObj::Way(w) => way_filter(w),
            OsmObj::Relation(r) => rel_filter(r),
        })
        .expect("could not read ways and relations");

    osm_objs
        .values()
        .filter_map(|obj| {
            Some(match obj {
                OsmObj::Way(way) if way_filter(way) => build_way(obj, &osm_objs),
                OsmObj::Relation(rel) if rel_filter(rel) => build_relation(obj, &osm_objs),
                _ => return None,
            })
        })
        .for_each(|obj| handle_obj(obj, db));

    // .get_objs_and_deps_store(
    //     |obj| match obj {
    //         OsmObj::Node(o) => node_filter(o),
    //         OsmObj::Way(w) => way_filter(w),
    //         OsmObj::Relation(r) => rel_filter(r),
    //     },
    //     &mut db_nodes,
    // )
    // .expect("get_nodes: get_objs_and_deps_store failed");
}

/// Function to generate a position for a **way**. If the **way** is only composed of one **node**,
/// it'll return the latitude and longitude of this **node**. If there is more than one, it'll first
/// create a polygon and then get its centroid's latitude and longitude.
///
/// In case of error when generating the polygon, it'll return `None`.
fn get_way_lat_lon(sub_objs: &[&OsmObj]) -> Option<(f64, f64)> {
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
                db.insert(new_address(way.tags(), lat, lon));
            }
        }
        StoredObj::Relation(r, objs) => {
            let addr_name = match r.tags().iter().find(|t| t.0 == "name").map(|t| t.1) {
                Some(addr) => addr,
                None => unreachable!(),
            };
            for sub_obj in objs {
                match sub_obj {
                    StoredObj::Node(n) if n.tags().iter().any(is_valid_housenumber_tag) => {
                        match &*n {
                            OsmObj::Node(n) => {
                                let mut addr = new_address(&n.tags, n.lat(), n.lon());
                                addr.street = Some(addr_name.clone());
                                db.insert(addr);
                            }
                            _ => unreachable!(),
                        }
                    }
                    StoredObj::Way(w, nodes) if w.tags().iter().any(is_valid_housenumber_tag) => {
                        if let Some((lat, lon)) = get_way_lat_lon(&nodes) {
                            let mut addr = new_address(w.tags(), lat, lon);
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
/// import_addresses("some_file.pbf".as_ref(), "nodes.db".as_ref(), &mut db);
/// ```
pub fn import_addresses<T: CompatibleDB>(pbf_file: &Path, db: &mut T) {
    let count_before = db.get_nb_addresses();

    get_nodes(pbf_file, db);

    let count_after = db.get_nb_addresses();
    tprintln!(
        "[OSM] Added {} addresses (total: {})",
        count_after - count_before,
        count_after
    );
}

fn is_valid_housenumber_tag(tag_kv: (&String, &String)) -> bool {
    // Long "housenumber" values should be excluded as they probably don't represent a house number.
    // Example: "addr:housenumber=Cochin International Airport Limited"
    let (key, value) = tag_kv;
    key == "addr:housenumber" && value.len() <= MAX_VALID_HOUSENUMBER_LENGTH
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use tools::*;
//
//     #[test]
//     fn check_relations() {
//         let pbf_file = "test-files/osm_input.pbf";
//         let db_file = "check_relations.db";
//
//         let mut db = DB::new(db_file, 0, true).expect("Failed to initialize DB");
//         assert_eq!(db.get_nb_addresses(), 361);
//         let addr = db.get_address(2, "Place de la Forêt de Cruye");
//         assert_eq!(addr.len(), 1);
//         let _ = fs::remove_file(db_file); // we ignore any potential error
//     }
// }
