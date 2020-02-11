use geo::algorithm::contains::Contains;
use geo::{MultiPolygon, Point};

use tools::Address;

lazy_static! {
    static ref FRANCE_SHAPE: MultiPolygon<f64> = {
        let collection = geo_geojson::from_str(include_str!("data/france.json"))
            .expect("failed to parse shape for France");
        collection
            .into_iter()
            .next()
            .expect("found an empty collection for France")
            .into_multi_polygon()
            .expect("France should be a MultiPolygon")
    };
}

#[derive(Clone, Copy, Debug)]
pub enum Source {
    Osm,
    OpenAddress,
    Bano,
}

impl Source {
    fn priority(self) -> f64 {
        match self {
            Self::OpenAddress => 1.,
            Self::Osm => 2.,
            Self::Bano => 3.,
        }
    }

    pub fn filter(self, address: &Address) -> bool {
        match self {
            Self::Osm | Self::OpenAddress => {
                !FRANCE_SHAPE.contains(&Point::new(address.lon, address.lat))
            }
            Self::Bano => true,
        }
    }

    pub fn ranking(self, address: &Address) -> f64 {
        self.priority() + address.count_non_empty_fields() as f64 / (1. + Address::NB_FIELDS as f64)
    }
}
