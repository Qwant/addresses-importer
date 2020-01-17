use std::ffi::CString;

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
}
