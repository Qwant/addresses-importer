use std::ffi::OsStr;
use std::fs::{self, File};
use std::path::Path;

use csv::Reader;
use tools::{teprint, teprintln, tprintln, Address, CompatibleDB};

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct OpenAddress {
    pub id: String,
    pub street: String,
    pub postcode: String,
    pub district: String,
    pub region: String,
    pub city: String,
    pub number: String,
    pub unit: String,
    pub lat: f64,
    pub lon: f64,
}

impl Into<Address> for OpenAddress {
    fn into(self) -> Address {
        let filter_empty = |field: String| {
            if field.is_empty() {
                None
            } else {
                Some(field)
            }
        };

        Address {
            lat: self.lat,
            lon: self.lon,
            number: filter_empty(self.number),
            street: filter_empty(self.street),
            unit: filter_empty(self.unit),
            city: filter_empty(self.city),
            district: filter_empty(self.district),
            region: filter_empty(self.region),
            postcode: filter_empty(self.postcode),
        }
    }
}

impl From<Address> for OpenAddress {
    fn from(address: Address) -> Self {
        OpenAddress {
            lat: address.lat,
            lon: address.lon,
            number: address.number.unwrap_or_default(),
            street: address.street.unwrap_or_default(),
            unit: address.unit.unwrap_or_default(),
            city: address.city.unwrap_or_default(),
            district: address.district.unwrap_or_default(),
            region: address.region.unwrap_or_default(),
            postcode: address.postcode.unwrap_or_default(),
            id: String::new(),
        }
    }
}

fn read_csv<P: AsRef<Path>, T: CompatibleDB>(db: &mut T, file_path: P) {
    let file = File::open(&file_path).expect("cannot open file");
    let mut rdr = Reader::from_reader(file);

    for address in rdr.deserialize::<OpenAddress>() {
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
        } else if path.extension().unwrap_or_else(|| OsStr::new("")) == "csv" {
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
