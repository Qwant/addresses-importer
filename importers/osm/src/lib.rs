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

use tools::{teprint, tprintln, Address, CompatibleDB};

/// Size of the read buffer put on top of the input PBF file
const PBF_BUFFER_SIZE: usize = 1024 * 1024; // 1MB

/// While reading the PBF, some objects for which members have not been fetched yet are kept into
/// memory. This is the maximal number of objects that are explicitly loaded from the PBF, value 5M
/// usually leads to less than 10GB of ram usage.
const MAX_PENDING_OBJECTS: usize = 5_000_000;

/// Used to make the stored elements in the first lighter by removing all the unused tags.
const REL_TAGS_TO_KEEP: &[&str] = &["name"];
const WAY_TAGS_TO_KEEP: &[&str] = &["addr:housenumber", "addr:street"];
const NODE_TAGS_TO_KEEP: &[&str] = &[
    "addr:housenumber",
    "addr:street",
    "addr:unit",
    "addr:city",
    "addr:district",
    "addr:region",
    "addr:postcode",
    "name",
];

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

/// Pack an OSM object together with the objects it depends on.
#[derive(Clone, Debug)]
struct DepObj {
    root: OsmObj,
    children: Vec<DepObj>,
}

impl DepObj {
    /// Check if all the children for this node have been extracted from OSM file.
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

    /// Reorder children with respect to OSM specified order, then it will
    /// remove the list of children ID's from the original object in order to
    /// free some RAM.
    ///
    /// This method must only be used when an object is complete as it would
    /// overwise destroy objects hierarchy.
    fn reorder_and_cleanup_children(&mut self) {
        // Execute some kind of radix sort: first we order objects by id
        let mut as_map: FxHashMap<OsmId, Vec<DepObj>> = FxHashMap::default();

        for obj in self.children.drain(..) {
            as_map.entry(obj.root.id()).or_default().push(obj);
        }

        // Then we fetch objects in order based on their ID
        self.children = self
            .expected_children()
            .filter_map(move |id| as_map.get_mut(&id)?.pop())
            .collect();

        self.children.shrink_to_fit();

        // Cleanup original object dependencies
        match &mut self.root {
            OsmObj::Node(_) => {}
            OsmObj::Way(w) => {
                std::mem::take(&mut w.nodes);
            }
            OsmObj::Relation(r) => {
                std::mem::take(&mut r.refs);
            }
        }
    }
}

impl From<OsmObj> for DepObj {
    fn from(mut obj: OsmObj) -> Self {
        let tags_to_keep = match obj {
            OsmObj::Node(_) => NODE_TAGS_TO_KEEP,
            OsmObj::Way(_) => WAY_TAGS_TO_KEEP,
            OsmObj::Relation(_) => REL_TAGS_TO_KEEP,
        };

        let tags = match &mut obj {
            OsmObj::Node(n) => &mut n.tags,
            OsmObj::Way(w) => &mut w.tags,
            OsmObj::Relation(r) => &mut r.tags,
        };

        tags.retain(|k, _| tags_to_keep.contains(&k.as_str()));
        tags.shrink_to_fit();

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

/// Compute depth of given object in dependency graph: an object with no parent
/// is of depth 1, its child are of depth 2, etc...
fn object_depth(obj_id: OsmId, deps_graph: &FxHashMap<OsmId, (u8, Vec<OsmId>)>) -> u8 {
    deps_graph.get(&obj_id).map(|x| x.0).unwrap_or(1)
}

/// Fetch all objects from input PBF reader. The objects that validate the function `filter_obj`
/// will be passed into `handle_obj` with their dependencies.
///
/// This is done by maintaining a list of objects into memory together with the graph of the
/// objects they depend on. The PBF files will be read sequentially several times until all
/// dependencies are resolved. To preserve from very high memory usage, the objects are deallocated
/// as soon as all their dependencies are resolved.
fn fetch_objects<R: BufRead + Seek, T: CompatibleDB>(
    max_depth: u8,
    reader: &mut OsmPbfReader<R>,
    filter_obj: impl Fn(&OsmObj) -> bool,
    db: &mut T,
) {
    // Simple counter to objects extracted so far
    let mut count_objs: u64 = 0;

    // Store objects that have not finished being built yet
    let mut pending: FxHashMap<OsmId, DepObj> = FxHashMap::default();

    // Store dependancy of one object to another
    let mut deps_graph: FxHashMap<OsmId, (u8, Vec<OsmId>)> = FxHashMap::default();

    // ID of the last explicitly imported object (excludes objects that are picked as a dependancy)
    let mut last_imported_object = None;

    // When this is true, then we import addresses that validate the filter, this is set to false
    // when the graph may take too much RAM
    let mut import_first_layer = true;

    // Check if current iteration made progress
    let mut made_progress = true;

    while made_progress {
        teprint!("Build graph layer ... ");
        made_progress = false;
        reader.rewind().expect("could not rewind PBF reader");

        'read_pbf: for obj in reader.par_iter() {
            let obj = obj.expect("could not read pbf");

            // The first layer only consists of filtered objects. Next layers include objects that
            // are required by dependency and not yet pending
            let feasible = (import_first_layer && filter_obj(&obj))
                || (deps_graph.contains_key(&obj.id()) && !pending.contains_key(&obj.id()));

            // Keep track of the last explicitly imported object while import_first_layer is true.
            // Then, it is set to true when the run reaches a section of the file that is not
            // imported yet.
            if import_first_layer {
                last_imported_object = Some(obj.id());
            } else if last_imported_object == Some(obj.id()) {
                import_first_layer = true;
            }

            if !feasible {
                continue;
            }

            // Convert into internal object format
            let obj: DepObj = obj.into();
            made_progress = true;

            // Create dependencies to this object, if they are within selected depth
            if object_depth(obj.root.id(), &deps_graph) < max_depth {
                for child in obj.expected_children() {
                    deps_graph
                        .entry(child)
                        .or_insert_with(|| (1, Vec::new()))
                        .1
                        .push(obj.root.id());
                }
            }

            // Start a graph search from current node, which propagate on completed objects
            let mut todo = vec![obj];

            while let Some(mut obj) = todo.pop() {
                if obj.is_complete() || object_depth(obj.root.id(), &deps_graph) == max_depth {
                    obj.reorder_and_cleanup_children();

                    if let Some((_, parents_id)) = deps_graph.remove(&obj.root.id()) {
                        // Insert the object in its parents
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

                            // Note that this clone should be OK because repeating child will
                            // normally only happen for closed ways.
                            parent_obj.children.push(obj.clone());
                            todo.push(parent_obj);
                        }
                    } else {
                        // If this object has no parents it means that it was selected by input
                        // filter and must be handled
                        handle_obj(obj, db, None);
                        count_objs += 1;
                    }
                } else {
                    pending.insert(obj.root.id(), obj);
                }
            }

            // Reset run if too many items are in dependencies
            if import_first_layer && pending.len() >= MAX_PENDING_OBJECTS {
                break 'read_pbf;
            }
        }

        import_first_layer = false;

        if made_progress {
            eprintln!(
                "{} pending, {} deps, objs: {}",
                pending.len(),
                deps_graph.len(),
                count_objs
            );
        } else {
            eprintln!("done");
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
fn handle_obj<T: CompatibleDB>(obj: DepObj, db: &mut T, override_street: Option<&str>) {
    let mut address = {
        match obj.root {
            OsmObj::Node(n) => new_address(&n.tags, n.lat(), n.lon()),
            OsmObj::Way(way) => {
                if let Some((lat, lon)) = get_way_lat_lon(&obj.children) {
                    new_address(&way.tags, lat, lon)
                } else {
                    return;
                }
            }
            OsmObj::Relation(r) => {
                if let Some(addr_name) = r.tags.iter().find(|t| t.0 == "name").map(|(_, n)| n) {
                    for sub_obj in obj.children {
                        handle_obj(sub_obj, db, Some(addr_name));
                    }
                }

                return;
            }
        }
    };

    if let Some(street) = override_street {
        address.street = Some(street.to_string());
    }

    db.insert(address);
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
/// import_addresses("some_file.pbf".as_ref(), &mut db);
/// ```
pub fn import_addresses<T: CompatibleDB>(pbf_file: &Path, db: &mut T) {
    let count_before = db.get_nb_addresses();

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
    fetch_objects(3, &mut reader, filter_obj, db);

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

#[cfg(test)]
mod tests {
    use super::*;
    use tools::*;

    #[test]
    fn check_relations() {
        let db_file = "check_relations.db";
        let mut db = DB::new(db_file, 0, true).expect("Failed to initialize DB");

        let pbf_file = "test-files/osm_input.pbf";
        import_addresses(pbf_file.as_ref(), &mut db);
        assert_eq!(db.get_nb_addresses(), 361);

        let addr = db.get_address(2, "Place de la Forêt de Cruye");
        assert_eq!(addr.len(), 1);

        let _ = std::fs::remove_file(db_file); // we ignore any potential error
    }
}
