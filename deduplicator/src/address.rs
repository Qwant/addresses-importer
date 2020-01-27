use std::ffi::CString;

use rusqlite::Row;

#[derive(Debug)]
pub struct Address {
    pub lat: f64,
    pub lon: f64,
    pub number: Option<String>,
    pub street: Option<String>,
    pub unit: Option<String>,
    pub city: Option<String>,
    pub district: Option<String>,
    pub region: Option<String>,
    pub postcode: Option<String>,
}

macro_rules! address_from_sqlite_row_with_prefix {
    ($prefix:expr, $row:expr) => {
        rusqlite::Result::Ok(Address {
            lat: $row.get(concat!($prefix, "lat"))?,
            lon: $row.get(concat!($prefix, "lon"))?,
            number: $row.get(concat!($prefix, "number"))?,
            street: $row.get(concat!($prefix, "street"))?,
            unit: $row.get(concat!($prefix, "unit"))?,
            city: $row.get(concat!($prefix, "city"))?,
            district: $row.get(concat!($prefix, "district"))?,
            region: $row.get(concat!($prefix, "region"))?,
            postcode: $row.get(concat!($prefix, "postcode"))?,
        })
    };
}

impl Address {
    pub fn to_postal_repr(&self) -> Vec<rpostal::Address> {
        [
            ("house_number", &self.number),
            ("road", &self.street),
            ("unit", &self.unit),
            ("city", &self.city),
            ("state_district", &self.district),
            ("country_region", &self.region),
            ("postcode", &self.postcode),
        ]
        .into_iter()
        .filter_map(|(key, val)| {
            val.as_ref().map(|val| rpostal::Address {
                label: CString::new(key.as_bytes()).unwrap(),
                value: CString::new(val.as_bytes()).unwrap(),
            })
        })
        .collect()
    }

    pub fn from_sqlite_row(row: &Row) -> rusqlite::Result<Self> {
        address_from_sqlite_row_with_prefix!("", row)
    }
}

impl Default for Address {
    fn default() -> Self {
        Address {
            lat: 0.,
            lon: 0.,
            number: None,
            street: None,
            unit: None,
            city: None,
            district: None,
            region: None,
            postcode: None,
        }
    }
}
