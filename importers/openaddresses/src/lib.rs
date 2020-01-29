use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::path::Path;
use std::str::FromStr;

use csv::{Reader, StringRecord};
use tools::{Address, CompatibleDB};

macro_rules! get_with_headers {
    ($headers:expr, $key:expr, $records:expr) => {
        if let Some(index) = $headers.0.get($key) {
            $records.get(*index)
        } else {
            None
        }
    };
}

macro_rules! get_f64 {
    ($headers:expr, $key:expr, $records:expr) => {
        match get_with_headers!($headers, $key, $records)
            .and_then(|x| f64::from_str(x).ok()) {
            Some(x) => x,
            None => continue,
        }
    };
}

struct Headers(HashMap<String, usize>);

impl Headers {
    fn new(headers: &StringRecord) -> Headers {
        let mut this = Headers(HashMap::with_capacity(11));

        for (pos, header) in headers.iter().enumerate() {
            this.0.insert(header.to_lowercase(), pos);
        }
        this
    }
}

fn read_csv<P: AsRef<Path>, T: CompatibleDB>(db: &mut T, file_path: P) {
    let file = File::open(file_path).expect("cannot open file");
    let mut rdr = Reader::from_reader(file);
    let headers = Headers::new(&rdr.headers().expect("no headers found"));

    let mut records = rdr.into_records();
    while let Some(x) = records.next() {
        let x = match x {
            Ok(x) => x,
            Err(e) => {
                eprintln!("invalid record found: {}", e);
                continue;
            }
        };

        db.insert(Address {
            lat: get_f64!(headers, "lat", x),
            lon: get_f64!(headers, "lon", x),
            number: get_with_headers!(headers, "number", x).map(|x| x.to_owned()),
            street: get_with_headers!(headers, "street", x).map(|x| x.to_owned()),
            unit: get_with_headers!(headers, "unit", x).map(|x| x.to_owned()),
            city: get_with_headers!(headers, "city", x).map(|x| x.to_owned()),
            district: get_with_headers!(headers, "district", x).map(|x| x.to_owned()),
            region: get_with_headers!(headers, "region", x).map(|x| x.to_owned()),
            postcode: get_with_headers!(headers, "postcode", x).map(|x| x.to_owned()),
        });
    }
}

pub fn import_addresses<P: AsRef<Path>, T: CompatibleDB>(path: P, db: &mut T) {
    for entry in fs::read_dir(path).expect("folder not found") {
        if let Ok(entry) = entry {
            let path = entry.path();
            if path.is_dir() {
                import_addresses(&path, db);
            } else if path.extension().unwrap_or_else(|| OsStr::new("")) == "csv" {
                read_csv(db, &path);
            }
        }
    }
}
