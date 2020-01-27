use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::iter;
use std::mem::drop;
use std::path::PathBuf;
use std::thread;

use crossbeam_channel as channel;
use geo::prelude::*;
use geo::Point;
use prog_rs::prelude::*;
use rpostal;
use rusqlite::DropBehavior;

use crate::address::Address;
use crate::db_hashes::{DbHashes, HashIterItem};
use crate::utils::is_constraint_violation_error;

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

    pub fn load_addresses<R>(
        &mut self,
        addresses: impl Iterator<Item = Address>,
        ranking: R,
    ) -> rusqlite::Result<()>
    where
        R: Fn(&Address) -> f64 + Clone + Send + 'static,
    {
        // Compute hashes in parallel using following pipeline:
        //
        // [     addr_sender      ] main thread
        //            |
        //            |  address
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
            let ranking = ranking.clone();

            thread::spawn(move || {
                for address in addr_receiver {
                    let rank = ranking(&address);
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
                let addr_id = inserter.insert_address(&address, rank);

                match addr_id {
                    Ok(addr_id) => {
                        for hash in hashes {
                            inserter
                                .insert_hash(addr_id, hash as i64)
                                .map_err(|err| {
                                    if !is_constraint_violation_error(&err) {
                                        eprintln!("failed inserting hash: {}", err);
                                    }
                                })
                                .ok();
                        }
                    }
                    Err(err) if !is_constraint_violation_error(&err) => {
                        eprintln!("failed inserting address: {}", err);
                    }
                    _ => (),
                }
            }
        });

        // --- Send addresses through channel

        for address in addresses {
            addr_sender
                .send(address)
                .expect("failed sending address: channel may have closed to early");
        }

        drop(addr_sender);
        writter_thread
            .join()
            .expect("failed joining writter thread");
        Ok(())
    }

    pub fn compute_duplicates(&mut self) -> rusqlite::Result<()> {
        println!("Build index on hashes");
        self.db.create_hashes_index()?;

        // --- Query collisions from DB
        let count_addresses_before = self.db.count_addresses()?;
        let count_hashes = self.db.count_hashes()?;

        println!(
            "Compute hash collisions ({} addresses, {} hashes)",
            count_addresses_before, count_hashes
        );

        let conn_get_collisions = self.db.get_conn()?;
        let mut sorted_hashes = DbHashes::get_sorted_hashes(&conn_get_collisions)?;

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
        let (col_sender, col_receiver) = channel::bounded::<Vec<HashIterItem>>(CHANNEL_SIZES);
        let (del_sender, del_receiver) = channel::bounded(CHANNEL_SIZES);

        // --- Init worker threads

        for _ in 0..nb_workers {
            let col_receiver = col_receiver.clone();
            let del_sender = del_sender.clone();

            thread::spawn(move || {
                for mut pack in col_receiver {
                    if pack.len() > 1000 {
                        eprintln!("Skipping pack of length {}", pack.len());
                        continue;
                    }

                    pack.sort_by(|pack_1, pack_2| {
                        (pack_1.rank, pack_1.id)
                            .partial_cmp(&(pack_2.rank, pack_2.id))
                            .unwrap_or_else(|| pack_1.id.cmp(&pack_2.id))
                    });

                    for j in 0..pack.len() {
                        for i in 0..j {
                            if is_duplicate(&POSTAL_CLASSIFIER, &pack[i].address, &pack[j].address)
                            {
                                del_sender.send(pack[i].id).expect(
                                    "failed sending id to delet: channel may have closed to early",
                                );
                            }
                        }
                    }

                    del_sender.send(0).unwrap();
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

        // Pack conflicting items together
        let conflicting_packs = sorted_hashes
            .iter()?
            .progress()
            .with_iter_size(count_hashes as usize)
            .filter_map(|item| {
                item.map_err(|err| eprintln!("failed retrieving hash: {}", err))
                    .ok()
            })
            .chain(iter::once(HashIterItem::default()))
            .scan(Vec::new(), |pack: &mut Vec<HashIterItem>, item| {
                Some({
                    if pack.first().map(|addr| addr.hash) == Some(item.hash) {
                        pack.push(item);
                        None
                    } else if pack.len() > 1 {
                        Some(std::mem::replace(pack, vec![item]))
                    } else {
                        pack.clear();
                        None
                    }
                })
            })
            .filter_map(|x| x);

        for pack in conflicting_packs {
            col_sender
                .send(pack)
                .expect("failed to send collision: channel may have closed too early");
        }

        drop(col_sender);
        writter_thread
            .join()
            .expect("failed joining writting thread");
        Ok(())
    }

    pub fn apply_and_clean(&self) -> rusqlite::Result<()> {
        println!(
            "Appling deletion ({} addresses)",
            self.db.count_to_delete()?
        );
        self.db.apply_addresses_to_delete()?;

        println!("Cleaning database");
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
    use rpostal::DuplicateStatus::*;
    let def_opt = POSTAL_CLASSIFIER.get_default_duplicate_options();

    let close_duplicate = || {
        let point_1 = Point::new(addr_1.lon, addr_1.lat);
        let point_2 = Point::new(addr_2.lon, addr_2.lat);

        (point_1.haversine_distance(&point_2) <= 100.)
            && field_compare(&addr_1.number, &addr_2.number, |x, y| {
                POSTAL_CLASSIFIER.is_house_number_duplicate(x, y, &def_opt) >= ExactDuplicate
            })
            && field_compare(&addr_1.street, &addr_2.street, |x, y| {
                POSTAL_CLASSIFIER.is_street_duplicate(x, y, &def_opt) >= LikelyDuplicate
            })
    };

    let exact_duplicate = || {
        field_compare(&addr_1.number, &addr_1.number, |x, y| {
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
