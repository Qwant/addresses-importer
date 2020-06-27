use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use geo::prelude::*;
use geo::Point;
use once_cell::{sync, unsync};
use tools::Address;

use crate::utils::{field_compare, opt_field_compare, postal_repr};

/// 5 seems to be a nice value for our use of libpostal: two addresses will be a collision if there
/// are distant of less than about 10km on the equator, and about 1km at a latitude of 80°.
///
/// Note that there is no city at less than 8° from a pole:
/// https://en.wikipedia.org/wiki/Alert,_Nunavut).
const GEOHASH_PRECISION: u32 = 5;

/// LibPostal instance
static POSTAL_CORE: sync::Lazy<rpostal::Core> =
    sync::Lazy::new(|| rpostal::Core::setup().expect("failed to init libpostal core"));

/// LibPostal classifier instance
static POSTAL_CLASSIFIER: sync::Lazy<rpostal::LanguageClassifier<'static>> =
    sync::Lazy::new(|| {
        POSTAL_CORE
            .setup_language_classifier()
            .expect("failed to init libpostal classifier")
    });

/// Return a sequence of hashes representing input address.
///
/// This hash function is built such that two addresses with both lexical and geographical
/// proximity are in collision.
///
/// # Example
/// ```
/// use deduplicator::dedupe::*;
/// use std::collections::HashSet;
/// use tools::Address;
///
/// let addr_1 = Address {
///     lat: 48.8707572,
///     lon: 2.3047277,
///     number: Some("32".to_string()),
///     street: Some("av. des Champs Élysées".to_string()),
///     ..Address::default()
/// };
///
/// let addr_2 = Address {
///     lat: 48.870,
///     lon: 2.304,
///     number: Some("32".to_string()),
///     street: Some("avenue des champs élysées".to_string()),
///     ..Address::default()
/// };
///
/// let hashes_1: HashSet<_> = hash_address(&addr_1).collect();
/// let hashes_2: HashSet<_> = hash_address(&addr_2).collect();
/// assert_ne!(hashes_1.intersection(&hashes_2).count(), 0);
/// ```
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

/// Check if two addresses are considered to be duplicates.
///
/// Current criteria for addresses to be duplicates is as follows:
///
/// - The distance between the two addresses is less than 100 meters and according
///   to libpostal and:
///     - have the same house number
///     - are likely to be in the same street (if there is less than 10 meters
///       between the two addresses, libpostal is allowed to only output
///       `PossibleDuplicate` for street name)
///
/// - According to libpostal, the two addresses have:
///     - the same house number
///     - the same street name
///     - the same city name
///     - the same postal code
///     - they are distant of less than 1km
///
/// # Example
/// ```
/// use deduplicator::dedupe::*;
/// use tools::Address;
///
/// let addr_1 = Address {
///     lat: 48.8707572,
///     lon: 2.3047277,
///     number: Some("32".to_string()),
///     street: Some("av. des Champs Élysées".to_string()),
///     ..Address::default()
/// };
///
/// let addr_2 = Address {
///     lat: 48.870,
///     lon: 2.304,
///     number: Some("32".to_string()),
///     street: Some("avenue des champs élysées".to_string()),
///     ..Address::default()
/// };
///
/// assert!(is_duplicate(&addr_1, &addr_2));
/// ```
pub fn is_duplicate(addr_1: &Address, addr_2: &Address) -> bool {
    use rpostal::DuplicateStatus::*;
    let def_opt = POSTAL_CLASSIFIER.get_default_duplicate_options();

    let point_1 = Point::new(addr_1.lon, addr_1.lat);
    let point_2 = Point::new(addr_2.lon, addr_2.lat);
    let dist = point_1.haversine_distance(&point_2);

    let is_house_number_duplicate = unsync::Lazy::new(|| {
        opt_field_compare(&addr_1.number, &addr_2.number, |x, y| {
            if x == y {
                ExactDuplicate
            } else {
                POSTAL_CLASSIFIER.is_house_number_duplicate(x, y, &def_opt)
            }
        })
    });

    let is_street_duplicate = unsync::Lazy::new(|| {
        field_compare(&addr_1.street, &addr_2.street, |x, y| {
            if x == y {
                ExactDuplicate
            } else {
                POSTAL_CLASSIFIER.is_street_duplicate(x, y, &def_opt)
            }
        })
    });

    let is_name_duplicate = unsync::Lazy::new(|| {
        field_compare(&addr_1.city, &addr_2.city, |x, y| {
            if x == y {
                ExactDuplicate
            } else {
                POSTAL_CLASSIFIER.is_name_duplicate(x, y, &def_opt)
            }
        })
    });

    let is_postal_code_duplicate = unsync::Lazy::new(|| {
        field_compare(&addr_1.postcode, &addr_2.postcode, |x, y| {
            if x == y {
                ExactDuplicate
            } else {
                POSTAL_CLASSIFIER.is_postal_code_duplicate(x, y, &def_opt)
            }
        })
    });

    let very_close_duplicate = || {
        dist < 10.
            && *is_house_number_duplicate >= ExactDuplicate
            && *is_street_duplicate >= PossibleDuplicateNeedsReview
    };

    let close_duplicate = || {
        dist < 100.
            && *is_house_number_duplicate >= ExactDuplicate
            && *is_street_duplicate >= LikelyDuplicate
    };

    let exact_duplicate = || {
        dist < 1000.
            && *is_house_number_duplicate == ExactDuplicate
            && *is_name_duplicate == ExactDuplicate
            && *is_postal_code_duplicate == ExactDuplicate
            && *is_street_duplicate == ExactDuplicate
    };

    very_close_duplicate() || close_duplicate() || exact_duplicate()
}
