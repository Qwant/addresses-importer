use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use geo::prelude::*;
use geo::Point;
use importer_tools::Address;
use rpostal;

use crate::utils::{field_compare, opt_field_compare, postal_repr};

/// 5 seems to be a nice value for our use of libpostal: two addresses will be a collision if there
/// are distant of less than about 10km on the equator, and about 1km at a latitude of 80°.
///
/// Note that there is no city at less than 8° from a pole:
/// https://en.wikipedia.org/wiki/Alert,_Nunavut).
const GEOHASH_PRECISION: u32 = 5;

lazy_static! {
    static ref POSTAL_CORE: rpostal::Core =
        rpostal::Core::setup().expect("failed to init libpostal core");
    static ref POSTAL_CLASSIFIER: rpostal::LanguageClassifier<'static> = POSTAL_CORE
        .setup_language_classifier()
        .expect("failed to init libpostal classifier");
}

pub fn hash_address(address: &Address) -> impl Iterator<Item = u64> {
    let options = rpostal::NearDupeHashOptions {
        // Only keep local keys (number / street), the geohash will filter distant addresses.
        address_only_keys: true,
        with_name: true,
        with_address: true,
        with_city_or_equivalent: false,
        with_postal_code: false,

        with_latlon: true,
        longitude: address.lon,
        latitude: address.lat,

        geohash_precision: GEOHASH_PRECISION,
        ..POSTAL_CLASSIFIER.get_near_dupe_hash_default_options()
    };

    POSTAL_CLASSIFIER
        .near_dupe_hashes(&postal_repr(address), &options)
        .into_iter()
        .map(|pre_hash| {
            let mut hash = DefaultHasher::new();
            pre_hash.hash(&mut hash);
            hash.finish()
        })
}

pub fn is_duplicate(addr_1: &Address, addr_2: &Address) -> bool {
    use rpostal::DuplicateStatus::*;
    let def_opt = POSTAL_CLASSIFIER.get_default_duplicate_options();

    let close_duplicate = || {
        let point_1 = Point::new(addr_1.lon, addr_1.lat);
        let point_2 = Point::new(addr_2.lon, addr_2.lat);

        (point_1.haversine_distance(&point_2) <= 100.)
            && opt_field_compare(&addr_1.number, &addr_2.number, |x, y| {
                POSTAL_CLASSIFIER.is_house_number_duplicate(x, y, &def_opt) >= ExactDuplicate
            })
            && field_compare(&addr_1.street, &addr_2.street, |x, y| {
                POSTAL_CLASSIFIER.is_street_duplicate(x, y, &def_opt) >= LikelyDuplicate
            })
    };

    let exact_duplicate = || {
        // using "// -" to force rustfmt output
        opt_field_compare(&addr_1.number, &addr_1.number, |x, y| {
            POSTAL_CLASSIFIER.is_house_number_duplicate(x, y, &def_opt) == ExactDuplicate
        }) // -
        && field_compare(&addr_1.street, &addr_2.street, |x, y| {
            POSTAL_CLASSIFIER.is_street_duplicate(x, y, &def_opt) == ExactDuplicate
        }) // -
        && ( // -
            field_compare(&addr_1.postcode, &addr_2.postcode, |x, y| {
                POSTAL_CLASSIFIER.is_postal_code_duplicate(x, y, &def_opt) == ExactDuplicate
            }) // -
            || field_compare(&addr_1.city, &addr_2.city, |x, y| {
                POSTAL_CLASSIFIER.is_name_duplicate(x, y, &def_opt) == ExactDuplicate
            })
        )
    };

    close_duplicate() || exact_duplicate()
}
