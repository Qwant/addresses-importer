use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use rpostal;

use crate::address::Address;

pub struct AddressHasher<'c> {
    rpostal_classifier: &'c rpostal::LanguageClassifier<'c>,
}

impl<'c> AddressHasher<'c> {
    pub fn new(rpostal_classifier: &'c rpostal::LanguageClassifier<'c>) -> Self {
        AddressHasher { rpostal_classifier }
    }

    pub fn hash_address(&self, address: &Address) -> impl Iterator<Item = u64> {
        let options = rpostal::NearDupeHashOptions {
            with_name: true,
            with_address: true,
            with_city_or_equivalent: true,
            longitude: address.lon,
            latitude: address.lat,
            with_latlon: true,
            address_only_keys: true,
            ..self.rpostal_classifier.get_near_dupe_hash_default_options()
        };

        self.rpostal_classifier
            .near_dupe_hashes(&address.to_postal_repr(), &options)
            .into_iter()
            .map(|pre_hash| {
                let mut hash = DefaultHasher::new();
                pre_hash.hash(&mut hash);
                hash.finish()
            })
    }
}
