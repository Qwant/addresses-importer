use std::cmp::min;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;
use std::path::PathBuf;

use geo::prelude::*;
use geo::Point;
use rayon::prelude::*;
use rpostal;
use rprogress::prelude::*;
use rusqlite::DropBehavior;

use crate::address::Address;
use crate::db_hashes::DbHashes;
use crate::postal_wrappers::{
    is_house_number_duplicate, is_name_duplicate, is_postal_code_duplicate, is_street_duplicate,
    DuplicateStatus,
};

const TRANSACTIONS_SIZE: usize = 1_000_000;

pub struct Dedupe {
    db: DbHashes,
    rpostal_core: rpostal::Core,
}

impl Dedupe {
    pub fn new(output_path: PathBuf) -> rusqlite::Result<Self> {
        Ok(Self {
            db: DbHashes::new(output_path)?,
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

        // Collect addresses into batches of size `TRANSACTIONS_SIZE`
        let addresses = addresses
            .scan(Vec::new(), |acc, address| {
                Some({
                    if acc.len() == TRANSACTIONS_SIZE {
                        println!();
                        Some(mem::replace(acc, vec![address]))
                    } else {
                        acc.push(address);
                        None
                    }
                })
            })
            .filter_map(|x| x);

        // Compute hashes for each batch
        let addresses_with_hashes = addresses.map(|address_batch| {
            address_batch
                .into_par_iter()
                .map(|address| {
                    let hashes: Vec<_> = hash_address(&rpostal_classifier, &address).collect();
                    (address, hashes)
                })
                .collect::<Vec<_>>()
        });

        // Write batches to output DB
        let mut conn = self.db.get_conn();

        for batch in addresses_with_hashes {
            let mut tran = conn.transaction()?;
            tran.set_drop_behavior(DropBehavior::Commit);
            let mut inserter = DbHashes::get_inserter(&mut tran)?;

            for (address, hashes) in batch
                .into_iter()
                .progress()
                .with_prefix(" -> writing output DB ... ".to_string())
            {
                if let Ok(addr_id) = inserter.insert_address(&address) {
                    for hash in hashes {
                        inserter.insert_hash(addr_id, hash as i64)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn compute_duplicates(&mut self) -> rusqlite::Result<()> {
        let conn_get_collisions = self.db.get_conn();

        let mut conn_insert = self.db.get_conn();
        let mut tran_insert = conn_insert.transaction()?;
        tran_insert.set_drop_behavior(DropBehavior::Commit);
        let mut inserter = DbHashes::get_inserter(&mut tran_insert)?;

        let count_addresses_before = self.db.count_addresses()?;
        eprintln!(
            "Query SQLite for hash collisions ({} addresses)",
            count_addresses_before
        );

        let rpostal_classifier = self
            .rpostal_core
            .setup_language_classifier()
            .expect("failed to init libpostal classifier");

        DbHashes::feasible_duplicates(&conn_get_collisions)?
            .iter()?
            .uprogress(0)
            .with_prefix("Filter real duplicates:".to_string())
            .map(|pair| pair.unwrap_or_else(|e| panic!("failed to retrieve duplicate: {}", e)))
            .filter(|((_id_1, addr_1), (_id_2, addr_2))| {
                is_duplicate(&rpostal_classifier, &addr_1, &addr_2)
            })
            .map(|((id_1, _addr_1), (id_2, _addr_2))| min(id_1, id_2))
            .for_each(|id_to_delete| {
                inserter.insert_to_delete(id_to_delete).unwrap();
            });

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
