use std::ffi::CString;

use rusqlite::Row;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
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
    pub const NB_FIELDS: usize = 9;

    pub fn count_non_empty_fields(&self) -> usize {
        2 + self.number.is_some() as usize
            + self.street.is_some() as usize
            + self.unit.is_some() as usize
            + self.city.is_some() as usize
            + self.district.is_some() as usize
            + self.region.is_some() as usize
            + self.postcode.is_some() as usize
    }

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

