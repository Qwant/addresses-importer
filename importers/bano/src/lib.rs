use std::fs::File;
use std::path::Path;
use std::str::FromStr;

use csv::ReaderBuilder;
use tools::{teprint, tprint, Address, CompatibleDB};

macro_rules! get {
    ($index:expr, $records:expr) => {
        $records.get($index)
    };
}

macro_rules! get_f64 {
    ($index:expr, $records:expr) => {
        match get!($index, $records).and_then(|x| f64::from_str(x).ok()) {
            Some(x) => x,
            None => continue,
        }
    };
}

pub fn import_addresses<P: AsRef<Path>, T: CompatibleDB>(file_path: P, db: &mut T) {
    tprint!("Reading `{}`...", file_path.as_ref().display());
    let file = File::open(file_path).expect("cannot open file");
    let rdr = ReaderBuilder::new().has_headers(false).from_reader(file);

    for x in rdr.into_records() {
        let x = match x {
            Ok(x) => x,
            Err(e) => {
                teprint!("invalid record found: {}", e);
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
    tprint!("Done!");
}
