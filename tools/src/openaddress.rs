use crate::Address;
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
