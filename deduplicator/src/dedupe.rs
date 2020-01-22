use std::cmp::min;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use geo::prelude::*;
use geo::Point;
use rpostal;
use rprogress::prelude::*;
use rusqlite::Connection;

use crate::address::Address;
use crate::db_hashes::DbHashes;
use crate::postal_wrappers::{
    is_house_number_duplicate, is_name_duplicate, is_postal_code_duplicate, is_street_duplicate,
    DuplicateStatus,
};

const TRANSACTIONS_SIZE: usize = 1_000_000;

pub struct Dedupe<'c> {
    db: DbHashes<'c>,
    rpostal_core: rpostal::Core,
}

impl<'c> Dedupe<'c> {
    pub fn new(output_conn: &'c Connection) -> rusqlite::Result<Self> {
        Ok(Self {
            db: DbHashes::new(output_conn)?,
            rpostal_core: rpostal::Core::setup().expect("failed to init libpostal"),
        })
    }

    pub fn load_addresses(
        &mut self,
        addresses: impl Iterator<Item = Address>,
    ) -> rusqlite::Result<()> {
        // Init libpostal classifier
        let rpostal_classifier = self
            .rpostal_core
            .setup_language_classifier()
            .expect("failed loading langage classifier");

        // Insert addresses
        self.db.begin_transaction()?;

        let mut nb_new_addresses = 0;
        let mut nb_old_addresses = 0;

        for (i, address) in addresses.enumerate() {
            if i % TRANSACTIONS_SIZE == 0 {
                self.db.commit_transaction()?;
                self.db.begin_transaction()?;
            }

            let row_id = match self.db.insert_address(&address) {
                Ok(address) => {
                    nb_new_addresses += 1;
                    address
                }
                Err(_) => {
                    nb_old_addresses += 1;
                    continue;
                }
            };

            for hash in hash_address(&rpostal_classifier, &address) {
                self.db.insert_hash(row_id, hash as i64)?;
            }
        }

        self.db.commit_transaction()?;
        eprintln!(
            "Imported {} new addresses, skipped {}",
            nb_new_addresses, nb_old_addresses
        );
        Ok(())
    }

    pub fn compute_duplicates(&mut self) -> rusqlite::Result<()> {
        let count_addresses_before = self.db.count_addresses()?;
        eprintln!(
            "Query SQLite for hash collisions ({} addresses)",
            count_addresses_before
        );

        let rpostal_classifier = self
            .rpostal_core
            .setup_language_classifier()
            .expect("failed to init libpostal classifier");

        let to_delete: HashSet<_> = self
            .db
            .feasible_duplicates()?
            .iter()?
            .map(|pair| pair.unwrap_or_else(|e| panic!("failed to retrieve duplicate: {}", e)))
            .filter(|((_id_1, addr_1), (_id_2, addr_2))| {
                is_duplicate(&rpostal_classifier, &addr_1, &addr_2)
            })
            // .inspect(|((id_1, addr_1), (id_2, addr_2))| {
            //     eprintln!("--- {}, {}\n{:?}\n{:?}", id_1, id_2, addr_1, addr_2)
            // })
            .map(|((id_1, _addr_1), (id_2, _addr_2))| min(id_1, id_2))
            .uprogress(0)
            .with_prefix("Filter real duplicates:".to_string())
            .collect();

        eprintln!(
            "{}/{} ({:.1}%) addresses will be delete",
            to_delete.len(),
            count_addresses_before,
            to_delete.len() as f32 / count_addresses_before as f32
        );

        self.db.begin_transaction()?;
        for address_id in to_delete
            .into_iter()
            .progress()
            .with_prefix("Register addresses to be deleted:".to_string())
        {
            self.db.insert_to_delete(address_id)?;
        }
        self.db.commit_transaction()?;

        Ok(())
    }

    pub fn apply_and_clean(&self) -> rusqlite::Result<()> {
        eprintln!("Appling deletion");
        self.db.apply_addresses_to_delete()?;

        eprintln!("Cleaning database");
        self.db.cleanup_database()?;

        Ok(())
    }
}

fn hash_address(
    rpostal_classifier: &rpostal::LanguageClassifier,
    address: &Address,
) -> impl Iterator<Item = u64> {
    let options = rpostal::NearDupeHashOptions {
        with_name: true,
        with_address: true,
        with_city_or_equivalent: true,
        longitude: address.lon,
        latitude: address.lat,
        with_latlon: true,
        address_only_keys: true,
        ..rpostal_classifier.get_near_dupe_hash_default_options()
    };

    rpostal_classifier
        .near_dupe_hashes(&address.to_postal_repr(), &options)
        .into_iter()
        .map(|pre_hash| {
            let mut hash = DefaultHasher::new();
            pre_hash.hash(&mut hash);
            hash.finish()
        })
}

fn field_compare<T>(
    field1: &Option<T>,
    field2: &Option<T>,
    compare: impl Fn(&T, &T) -> bool,
) -> bool {
    match (field1.as_ref(), field2.as_ref()) {
        (Some(field1), Some(field2)) => compare(field1, field2),
        _ => false,
    }
}

pub fn is_duplicate(
    _rpostal_classifier: &rpostal::LanguageClassifier,
    addr_1: &Address,
    addr_2: &Address,
) -> bool {
    use DuplicateStatus::*;

    let close_duplicate = || {
        let point_1 = Point::new(addr_1.lon, addr_1.lat);
        let point_2 = Point::new(addr_2.lon, addr_2.lat);

        (point_1.haversine_distance(&point_2) <= 100.)
            && field_compare(&addr_1.number, &addr_2.number, |x, y| {
                is_house_number_duplicate(x, y) >= ExactDuplicate
            })
            && field_compare(&addr_1.street, &addr_2.street, |x, y| {
                is_street_duplicate(x, y) >= LikelyDuplicate
            })
    };

    let exact_duplicate = || {
        field_compare(&addr_1.number, &addr_1.number, |x, y| {
            is_house_number_duplicate(x, y) == ExactDuplicate
        }) // -
        && field_compare(&addr_1.street, &addr_2.street, |x, y| {
            is_street_duplicate(x, y) == ExactDuplicate
        }) // -
        && (field_compare(&addr_1.postcode, &addr_2.postcode, |x, y| {
            is_postal_code_duplicate(x, y) == ExactDuplicate
        }) || field_compare(&addr_1.city, &addr_2.city, |x, y| {
            is_name_duplicate(x, y) == ExactDuplicate
        }))
    };

    close_duplicate() || exact_duplicate()
}
