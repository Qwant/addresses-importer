//! Specifications for different address sources.

use geo::algorithm::contains::Contains;
use geo::{Geometry, MultiPolygon, Point};
use geojson::GeoJson;
use once_cell::sync::Lazy;
use std::str::FromStr;

use tools::Address;

static FRANCE_SHAPE: Lazy<MultiPolygon<f64>> = Lazy::new(|| {
    let shape: Geometry<f64> = GeoJson::from_str(include_str!("data/france.json"))
        .expect("failed to parse shape for France")
        .try_into()
        .expect("France shape should be a Geometry");

    shape.try_into().expect("France should be a MultiPolygon")
});

/// A source of addresses.
#[derive(Clone, Copy, Debug)]
pub enum Source {
    Osm,
    OpenAddress,
    Bano,
}

impl Source {
    /// Get the base priority of the source.
    ///
    /// # Example
    /// ```
    /// use deduplicator::sources::*;
    ///
    /// // We expect Bano to have the best reliability and OpenAddress the worst.
    /// assert!(Source::OpenAddress.priority() < Source::Osm.priority());
    /// assert!(Source::Osm.priority() < Source::Bano.priority());
    /// ```
    pub fn priority(self) -> f64 {
        match self {
            Self::OpenAddress => 1.,
            Self::Osm => 2.,
            Self::Bano => 3.,
        }
    }

    /// Return false if an address should not be imported for this source.
    ///
    /// # Example
    /// ```
    /// use deduplicator::sources::*;
    /// use tools::Address;
    ///
    /// let addr_inside_paris = Address {
    ///     lat: 48.8,
    ///     lon: 2.3,
    ///     ..Address::default()
    /// };
    ///
    /// // French addresses should only be imported from Bano
    /// assert!(!&Source::OpenAddress.filter(&addr_inside_paris));
    /// assert!(!&Source::Osm.filter(&addr_inside_paris));
    /// assert!(&Source::Bano.filter(&addr_inside_paris));
    /// ```
    pub fn filter(self, address: &Address) -> bool {
        match self {
            Self::Osm | Self::OpenAddress => {
                !FRANCE_SHAPE.contains(&Point::new(address.lon, address.lat))
            }
            Self::Bano => true,
        }
    }

    /// Return the ranking of an address that originates from this source.
    ///
    /// # Example
    /// ```
    /// use deduplicator::sources::*;
    /// use tools::Address;
    ///
    /// let addr = Address::default();
    ///
    /// // The same address should be taken in priority from OSM
    /// assert!(Source::OpenAddress.ranking(&addr) < Source::Osm.ranking(&addr));
    /// ```
    pub fn ranking(self, address: &Address) -> f64 {
        self.priority() + address.count_non_empty_fields() as f64 / (1. + Address::NB_FIELDS as f64)
    }
}
