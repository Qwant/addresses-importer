use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem::drop;
use std::path::PathBuf;
use std::thread;

use crossbeam::channel;
use geo::prelude::*;
use geo::Point;
use rpostal;
use rusqlite::DropBehavior;

use crate::address::Address;
use crate::db_hashes::DbHashes;
use crate::postal_wrappers::{
    is_house_number_duplicate, is_name_duplicate, is_postal_code_duplicate, is_street_duplicate,
    DuplicateStatus,
};

const CHANNEL_SIZES: usize = 1000;

lazy_static! {
    static ref POSTAL_CORE: rpostal::Core =
        rpostal::Core::setup().expect("failed to init libpostal core");
    static ref POSTAL_CLASSIFIER: rpostal::LanguageClassifier<'static> = POSTAL_CORE
        .setup_language_classifier()
        .expect("failed to init libpostal classifier");
}

pub struct Dedupe {
    db: DbHashes,
}

impl Dedupe {
    pub fn new(output_path: PathBuf) -> rusqlite::Result<Self> {
        Ok(Self {
            db: DbHashes::new(output_path)?,
        })
    }

    pub fn load_addresses(
        &mut self,
        addresses: impl Iterator<Item = (Address, f64)>,
    ) -> rusqlite::Result<()> {
        // Compute hashes in parallel using following pipeline:
        //
        // [     addr_sender      ] main thread
        //            |
        //            |  (address, rank)
        //            v
        // [    addr_receiver     ]
        // [         |||          ] worker threads
        // [     hash_sender      ]
        //            |
        //            |  (address, rank, hashes)
        //            v
        // [     hash_receiver    ] writter thread

        let nb_workers: usize = num_cpus::get() - 2;
        let (addr_sender, addr_receiver) = channel::bounded(CHANNEL_SIZES);
        let (hash_sender, hash_receiver) = channel::bounded(CHANNEL_SIZES);

        // --- Init worker threads

        for _ in 0..nb_workers {
            let addr_receiver = addr_receiver.clone();
            let hash_sender = hash_sender.clone();

            thread::spawn(move || {
                for (address, rank) in addr_receiver {
                    let hashes: Vec<_> = hash_address(&POSTAL_CLASSIFIER, &address).collect();
                    hash_sender
                        .send((address, rank, hashes))
                        .expect("failed sending hashes: channel may have closed too early");
                }
            });
        }

        // Drop channels that were cloned before being sent
        drop(addr_receiver);
        drop(hash_sender);

        // --- Init writter thread

        let mut conn = self
            .db
            .get_conn()
            .expect("failed to open SQLite connection");
        let writter_thread = thread::spawn(move || {
            let mut tran = conn.transaction().expect("failed to init transaction");
            tran.set_drop_behavior(DropBehavior::Commit);
            let mut inserter = DbHashes::get_inserter(&mut tran).expect("failed to init inserter");

            for (address, rank, hashes) in hash_receiver {
                // TODO: better error handling
                if let Ok(addr_id) = inserter.insert_address(&address, rank) {
                    for hash in hashes {
                        inserter
                            .insert_hash(addr_id, hash as i64)
                            .expect("failed inserting hash");
                    }
                }
            }
        });

        // --- Send addresses through channel

        for (address, rank) in addresses {
            addr_sender
                .send((address, rank))
                .expect("failed sending address: channel may have closed to early");
        }

        drop(addr_sender);
        writter_thread
            .join()
            .expect("failed joining writter thread");
        Ok(())
    }

    pub fn compute_duplicates(&mut self) -> rusqlite::Result<()> {
        // --- Query collisions from DB
        let count_addresses_before = self.db.count_addresses()?;
        eprintln!(
            "Query SQLite for hash collisions ({} addresses)",
            count_addresses_before
        );

        let conn_get_collisions = self.db.get_conn()?;
        let mut collisions = DbHashes::feasible_duplicates(&conn_get_collisions)?;

        // Eliminate false positives in parallel using following pipeline:
        //
        // [     col_sender     ] main thread
        //            |
        //            |  (address, rank)
        //            v
        // [    col_receiver    ]
        // [         |||        ] worker threads
        // [     del_sender     ]
        //            |
        //            |  (address, rank, hashes)
        //            v
        // [    del_receiver    ] writter thread

        let nb_workers: usize = num_cpus::get() - 2;
        let (col_sender, col_receiver) = channel::bounded(CHANNEL_SIZES);
        let (del_sender, del_receiver) = channel::bounded(CHANNEL_SIZES);

        // --- Init worker threads

        for _ in 0..nb_workers {
            let col_receiver = col_receiver.clone();
            let del_sender = del_sender.clone();

            thread::spawn(move || {
                for ((id_1, addr_1, rank_1), (id_2, addr_2, rank_2)) in col_receiver {
                    if is_duplicate(&POSTAL_CLASSIFIER, &addr_1, &addr_2) {
                        let to_delete = if rank_1 > rank_2 { id_1 } else { id_2 };
                        del_sender.send(to_delete).expect(
                            "failed to send id to delete: channel may have closed too early",
                        );
                    }
                }
            });
        }

        drop(col_receiver);
        drop(del_sender);

        // --- Init writter thread

        let mut conn_insert = self.db.get_conn()?;

        let writter_thread = thread::spawn(move || {
            let mut tran_insert = conn_insert
                .transaction()
                .expect("failed to init transaction");
            tran_insert.set_drop_behavior(DropBehavior::Commit);
            let mut inserter =
                DbHashes::get_inserter(&mut tran_insert).expect("failed to init inserter");

            for id in del_receiver {
                if let Err(e) = inserter.insert_to_delete(id) {
                    eprintln!("failed to insert id to delete in the database: {}", e)
                };
            }
        });

        // --- Send conflicting pairs into channels

        for collision in collisions.iter()? {
            col_sender
                .send(collision?)
                .expect("failed to send collision: channel may have closed too early");
        }

        drop(col_sender);
        writter_thread
            .join()
            .expect("failed joining writting thread");
        Ok(())
    }

    pub fn apply_and_clean(&self) -> rusqlite::Result<()> {
        eprintln!(
            "Appling deletion ({} addresses)",
            self.db.count_to_delete()?
        );
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
        && ( // -
            field_compare(&addr_1.postcode, &addr_2.postcode, |x, y| {
                is_postal_code_duplicate(x, y) == ExactDuplicate
            }) // -
            || field_compare(&addr_1.city, &addr_2.city, |x, y| {
                is_name_duplicate(x, y) == ExactDuplicate
            })
        )
    };

    close_duplicate() || exact_duplicate()
}