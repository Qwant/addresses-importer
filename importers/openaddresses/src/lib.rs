use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;

use serde::Deserialize;
use smartstring::alias::String;
use tools::{teprint, teprintln, tprintln, Address, CompatibleDB};

/// Size of the read buffer put on top of the input CSV file
const GJ_BUFFER_SIZE: usize = 1024 * 1024; // 1MB

/// Subset of a Geojson Feature, expected to contain a point.
#[derive(Deserialize)]
pub struct OpenAddressFeature {
    // pub type: String,
    properties: OpenAddressProperties,
    geometry: OpenAddressGeometry,
}

/// Subset of a geojson geometry, expected to be a point
#[derive(Deserialize)]
pub struct OpenAddressGeometry {
    // pub type: String,
    pub coordinates: [f64; 2],
}

#[derive(Deserialize)]
pub struct OpenAddressProperties {
    // pub hash: String,
    // pub id: String,
    pub number: String,
    pub street: String,
    pub unit: String,
    pub city: String,
    pub district: String,
    pub region: String,
    pub postcode: String,
}

impl From<OpenAddressFeature> for Address {
    fn from(val: OpenAddressFeature) -> Self {
        let filter_empty = |field: String| {
            if field.is_empty() {
                None
            } else {
                Some(field)
            }
        };

        let [lon, lat] = val.geometry.coordinates;
        let props = val.properties;

        Address {
            lat,
            lon,
            number: filter_empty(props.number),
            street: filter_empty(props.street),
            unit: filter_empty(props.unit),
            city: filter_empty(props.city),
            district: filter_empty(props.district),
            region: filter_empty(props.region),
            postcode: filter_empty(props.postcode),
        }
    }
}

/// This function is called on every CSV file encountered in the given folder tree in the
/// `import_addresses` function. It simply reads it and fills the `db` object.
fn read_csv<P: AsRef<Path>, T: CompatibleDB>(db: &mut T, file_path: P) {
    let file = BufReader::with_capacity(
        GJ_BUFFER_SIZE,
        File::open(&file_path).expect("cannot open file"),
    );

    let rdr = serde_json::Deserializer::from_reader(file);

    for address in rdr.into_iter::<OpenAddressFeature>() {
        match address {
            Ok(address) => db.insert(address.into()),
            Err(err) => teprintln!(
                "[OA] Invalid record found in {:?}: {}",
                file_path.as_ref(),
                err
            ),
        }
    }
}

/// The entry point of the **OpenAddresses** importer.
///
/// * The `base_path` argument is where the top folder containing the CSV files is located.
/// * The `db` argument is the mutable database wrapper implementing the `CompatibleDB` trait where
///   the data will be stored.
///
/// Considering it's calling the `read_csv` function on every CSV files it finds, it could be pretty
/// simply run in parallel. It'd require `db` to be able to handle multi-threading though. To be
/// done later I guess?
///
/// Example:
///
/// ```no_run
/// use tools::DB;
/// use openaddresses::import_addresses;
///
/// let mut db = DB::new("addresses.db", 10000, true).expect("failed to create DB");
/// import_addresses("some_folder", &mut db);
/// ```
pub fn import_addresses<P: AsRef<Path>, T: CompatibleDB>(base_path: P, db: &mut T) {
    let count_before = db.get_nb_addresses();
    let mut count_after = count_before;

    let mut todo = vec![base_path.as_ref().to_path_buf()];

    while let Some(path) = todo.pop() {
        if path.is_dir() {
            fs::read_dir(path)
                .expect("folder not found")
                .filter_map(|item| {
                    item.map_err(|err| teprintln!("Failed to read path: {}", err))
                        .ok()
                })
                .for_each(|item| todo.push(item.path()));
        } else if path.extension().unwrap_or_else(|| OsStr::new("")) == "geojson" {
            let short_name = path.strip_prefix(&base_path).unwrap_or(&path);
            teprint!("[OA] Reading {:<40} ...\r", short_name.display());
            read_csv(db, &path);

            let new_count_after = db.get_nb_addresses();
            teprintln!(
                "[OA] Reading {:<40} ... {} addresses (total: {})",
                short_name.display(),
                new_count_after - count_after,
                new_count_after
            );

            count_after = new_count_after;
        }
    }

    tprintln!(
        "[OA] Added {} addresses (total: {})",
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
        let db_file = "check_relations.db";
        let mut db = DB::new(db_file, 0, true).expect("Failed to initialize DB");

        let gj_file = "data/sample.geojson";
        import_addresses(gj_file, &mut db);
        assert_eq!(db.get_nb_addresses(), 1000);

        let addr = db.get_address(38, "Allee du Chalam");
        assert_eq!(addr.len(), 1);
        assert_eq!(addr[0].lon, 5.802057);
        assert_eq!(addr[0].lat, 46.142921);

        let _ = std::fs::remove_file(db_file); // we ignore any potential error
    }
}
