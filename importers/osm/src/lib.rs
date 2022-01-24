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

use std::fs::File;
use std::io::{BufRead, BufReader, Seek};
use std::path::Path;

use fxhash::FxHashMap;
use geos::Geometry;
use osmpbfreader::objects::{OsmId, Tags};
use osmpbfreader::{OsmObj, OsmPbfReader};

use tools::{teprintln, Address, CompatibleDB};

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

/// Type used to pack an object with its dependancies.
#[derive(Clone, Debug)]
struct DepObj {
    root: OsmObj,
    children: Vec<DepObj>,
}

impl DepObj {
    /// Check if all the children for this node have been extracted from OSM.
    fn is_complete(&self) -> bool {
        self.children.len() == self.children.capacity()
    }

    /// Return an iterator over all id's of this object's children
    fn expected_children(&self) -> impl Iterator<Item = OsmId> + '_ {
        let way_children = (self.root.way())
            .into_iter()
            .flat_map(|way| way.nodes.iter().copied().map(Into::into));

        let rel_children = (self.root.relation())
            .into_iter()
            .flat_map(|way| way.refs.iter().map(|r| r.member));

        way_children.chain(rel_children)
    }

    /// Reorder children with respect to OSM specified order.
    fn reorder(&mut self) {
        assert!(self.is_complete());
        let len = self.children.len();

        let mut old_children = std::mem::take(&mut self.children);
        let mut new_children = Vec::with_capacity(len);

        new_children.extend(self.expected_children().into_iter().map(move |obj_id| {
            let child_pos = old_children
                .iter()
                .position(|obj| obj.root.id() == obj_id)
                .unwrap();

            old_children.swap_remove(child_pos)
        }));

        self.children = new_children;

        // let index: FxHashMap<OsmId, usize> = self
        //     .expected_children()
        //     .enumerate()
        //     .map(|(pos, obj)| (obj, pos))
        //     .collect();
        //
        // self.children
        //     .sort_unstable_by_key(|obj| index[&obj.root.id()]);
    }
}

impl From<OsmObj> for DepObj {
    fn from(obj: OsmObj) -> Self {
        let max_children = {
            match &obj {
                OsmObj::Node(_) => 0,
                OsmObj::Way(w) => w.nodes.len(),
                OsmObj::Relation(r) => r.refs.len(),
            }
        };

        Self {
            root: obj,
            children: Vec::with_capacity(max_children),
        }
    }
}

fn build_graph<R: BufRead + Seek, T: CompatibleDB>(
    depth: usize,
    reader: &mut OsmPbfReader<R>,
    filter_obj: impl Fn(&OsmObj) -> bool,
    db: &mut T,
) {
    // Store objects that have not finished being built yet
    let mut pending: FxHashMap<OsmId, DepObj> = FxHashMap::default();

    // Store dependancy of one object to another
    let mut graph: FxHashMap<OsmId, Vec<OsmId>> = FxHashMap::default();

    // Add next layers
    for layer in 1..=depth {
        teprintln!("Build graph layer {}", layer);
        reader.rewind().expect("could not rewind PBF reader");

        for obj in reader.par_iter() {
            let obj: DepObj = obj.expect("could not read pbf").into();

            // The first layer only consists of filtered objects
            // Next layers include objects that are required by dependancy and not yet pending
            let feasible = (layer == 1 && filter_obj(&obj.root))
                || (layer > 1
                    && graph.contains_key(&obj.root.id())
                    && !pending.contains_key(&obj.root.id()));

            if !feasible {
                continue;
            }

            // Create dependancies to this object
            for child in obj.expected_children() {
                graph.entry(child).or_default().push(obj.root.id());
            }

            // Start a graph search from current node, which propagate on completed objects
            let mut todo = vec![obj];

            while let Some(mut obj) = todo.pop() {
                if obj.is_complete() {
                    obj.reorder();

                    if let Some(parents_id) = graph.remove(&obj.root.id()) {
                        for parent_id in parents_id {
                            let mut parent_obj = {
                                if let Some(parent_obj) = pending.remove(&parent_id) {
                                    parent_obj
                                } else {
                                    let index_in_stack =
                                        todo.iter().position(|x| x.root.id() == parent_id).unwrap();
                                    todo.swap_remove(index_in_stack)
                                }
                            };
                            parent_obj.children.push(obj.clone()); // TODO: AIE AIE AIE
                            todo.push(parent_obj);
                        }
                    } else {
                        // If this object has no parents it means that it was filtered in
                        handle_obj(obj, db);
                    }
                } else {
                    pending.insert(obj.root.id(), obj);
                }
            }
        }
    }
}

/// Function to generate a position for a **way**. If the **way** is only composed of one **node**,
/// it'll return the latitude and longitude of this **node**. If there is more than one, it'll first
/// create a polygon and then get its centroid's latitude and longitude.
///
/// In case of error when generating the polygon, it'll return `None`.
fn get_way_lat_lon(sub_objs: &[DepObj]) -> Option<(f64, f64)> {
    let nodes: Vec<_> = sub_objs
        .iter()
        .map(|x| {
            x.root
                .node()
                .expect("nothing else than nodes should be in a way!")
        })
        .collect();

    if let [node] = nodes[..] {
        return Some((node.lat(), node.lon()));
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
fn handle_obj<T: CompatibleDB>(obj: DepObj, db: &mut T) {
    match obj.root {
        OsmObj::Node(n) => db.insert(new_address(&n.tags, n.lat(), n.lon())),
        OsmObj::Way(way) => {
            if let Some((lat, lon)) = get_way_lat_lon(&obj.children) {
                db.insert(new_address(&way.tags, lat, lon))
            }
        }
        OsmObj::Relation(r) => {
            let addr_name = r.tags.iter().find(|t| t.0 == "name").unwrap().1;

            for sub_obj in obj.children {
                match sub_obj.root {
                    OsmObj::Node(n) if n.tags.iter().any(is_valid_housenumber_tag) => {
                        let mut addr = new_address(&n.tags, n.lat(), n.lon());
                        addr.street = Some(addr_name.clone());
                        db.insert(addr);
                    }
                    OsmObj::Way(w) if w.tags.iter().any(is_valid_housenumber_tag) => {
                        if let Some((lat, lon)) = get_way_lat_lon(&sub_obj.children) {
                            let mut addr = new_address(&w.tags, lat, lon);
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
    let filter_obj = |obj: &OsmObj| match obj {
        OsmObj::Node(n) => {
            n.tags.iter().any(is_valid_housenumber_tag)
                && n.tags.iter().any(|x| x.0 == "addr:street")
        }
        OsmObj::Way(w) => {
            !w.nodes.is_empty()
                && w.tags.iter().any(is_valid_housenumber_tag)
                && w.tags.iter().any(|x| x.0 == "addr:street")
        }
        OsmObj::Relation(r) => {
            !r.refs.is_empty()
                && r.tags
                    .iter()
                    .any(|x| x.0 == "type" && x.1 == "associatedStreet")
                && r.tags.iter().any(|x| x.0 == "name")
        }
    };

    // Init reader
    let file = BufReader::with_capacity(
        PBF_BUFFER_SIZE,
        File::open(&pbf_file)
            .unwrap_or_else(|err| panic!("Failed to open file {:?}: {}", pbf_file, err)),
    );

    let mut reader = OsmPbfReader::new(file);

    // Build graph
    let _graph = build_graph(3, &mut reader, filter_obj, db);

    // tprintln!("Graph size: {}", graph.len());
    //
    // let count_before = db.get_nb_addresses();
    //
    // get_nodes(pbf_file, db);
    //
    // let count_after = db.get_nb_addresses();
    // tprintln!(
    //     "[OSM] Added {} addresses (total: {})",
    //     count_after - count_before,
    //     count_after
    // );
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
