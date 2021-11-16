use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::str::FromStr;

use csv::ReaderBuilder;
use tools::{teprintln, tprintln, Address, CompatibleDB};

/// Size of the read buffer put on top of the input PBF file
const CSV_BUFFER_SIZE: usize = 1024 * 1024; // 1MB

/// Helper macro to convert a CSV field into a `String`.
macro_rules! get {
    ($index:expr, $records:expr) => {
        $records.get($index)
    };
}

/// Helper macro to convert a CSV field into an `f64`.
macro_rules! get_f64 {
    ($index:expr, $records:expr) => {
        match get!($index, $records).and_then(|x| f64::from_str(x).ok()) {
            Some(x) => x,
            None => continue,
        }
    };
}

/// The entry point of the BANO importer.
///
/// * The `file_path` argument is where the BANO CSV file is located.
/// * The `db` argument is the mutable database wrapper implementing the `CompatibleDB` trait where
///   the data will be stored.
///
/// Example:
///
/// ```no_run
/// use tools::DB;
/// use bano::import_addresses;
///
/// let mut db = DB::new("addresses.db", 10000, true).expect("failed to create DB");
/// import_addresses("somefile.csv", &mut db);
/// ```
pub fn import_addresses<P: AsRef<Path>, T: CompatibleDB>(file_path: P, db: &mut T) {
    teprintln!("[BANO] Reading `{}`", file_path.as_ref().display());
    let count_before = db.get_nb_addresses();

    let file = BufReader::with_capacity(
        CSV_BUFFER_SIZE,
        File::open(file_path).expect("cannot open file"),
    );

    let rdr = ReaderBuilder::new().has_headers(false).from_reader(file);

    for x in rdr.into_records() {
        let x = match x {
            Ok(x) => x,
            Err(e) => {
                teprintln!("[BANO] Invalid record found: {}", e);
                continue;
            }
        };

        db.insert(Address {
            lat: get_f64!(6, x),
            lon: get_f64!(7, x),
            number: get!(1, x).map(|x| x.to_owned()),
            street: get!(2, x).map(|x| x.to_owned()),
            unit: None,
            city: get!(4, x).map(|x| x.to_owned()),
            district: None,
            region: None,
            postcode: get!(3, x).map(|x| x.to_owned()),
        });
    }

    let count_after = db.get_nb_addresses();
    tprintln!(
        "[BANO] Added {} addresses (total: {})",
        count_after - count_before,
        count_after
    );
}
