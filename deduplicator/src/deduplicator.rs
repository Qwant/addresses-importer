use std::mem::drop;
use std::path::PathBuf;
use std::thread;

use crossbeam_channel as channel;
use importer_tools::Address;
use itertools::Itertools;
use prog_rs::prelude::*;
use rusqlite::{Connection, DropBehavior};

use crate::db_hashes::{DbHashes, HashIterItem};
use crate::dedupe::{hash_address, is_duplicate};
use crate::utils::is_constraint_violation_error;

const CHANNEL_SIZES: usize = 1000;

pub struct Deduplicator {
    db: DbHashes,
}

impl Deduplicator {
    pub fn new(output_path: PathBuf) -> rusqlite::Result<Self> {
        Ok(Self {
            db: DbHashes::new(output_path)?,
        })
    }

    pub fn get_db_inserter<F, R>(&mut self, filter: F, ranking: R) -> rusqlite::Result<DbInserter>
    where
        F: Fn(&Address) -> bool + Clone + Send + 'static,
        R: Fn(&Address) -> f64 + Clone + Send + 'static,
    {
        Ok(DbInserter::new(self.db.get_conn()?, filter, ranking))
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

                    for j in 1..pack.len() {
                        for i in 0..j {
                            if is_duplicate(&pack[i].address, &pack[j].address) {
                                del_sender.send(pack[i].id).expect(
                                    "failed sending id to delete: channel may have closed to early",
                                );
                            }
                        }
                    }
                }
            });
        }

        // Drop channels that were cloned before being sent
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
                match inserter.insert_to_delete(id) {
                    Err(err) if !is_constraint_violation_error(&err) => {
                        eprintln!("failed to insert id to delete in the database: {}", err)
                    }
                    _ => (),
                }
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
            .group_by(|addr| addr.hash);

        // Remove packs of single elements
        let conflicting_packs = conflicting_packs
            .into_iter()
            .map(|(_key, pack)| pack.collect::<Vec<_>>())
            .filter(|pack| pack.len() >= 2);

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

    pub fn apply_and_clean(&self, keep_construction_tables: bool) -> rusqlite::Result<()> {
        println!(
            "Appling deletion ({} addresses)",
            self.db.count_to_delete()?
        );
        self.db.apply_addresses_to_delete()?;

        if !keep_construction_tables {
            println!("Cleaning database");
            self.db.cleanup_database()?;

            println!("Vacuum database");
            self.db.vacuum()?;
        }

        Ok(())
    }
}

//  ___                     _   _
// |_ _|_ __  ___  ___ _ __| |_(_) ___  _ __
//  | || '_ \/ __|/ _ \ '__| __| |/ _ \| '_ \
//  | || | | \__ \  __/ |  | |_| | (_) | | | |
// |___|_| |_|___/\___|_|   \__|_|\___/|_| |_|
//
//
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

pub struct DbInserter {
    addr_sender: channel::Sender<Address>,
    writter_thread: thread::JoinHandle<()>,
}

impl DbInserter {
    pub fn new<F, R>(mut conn: Connection, filter: F, ranking: R) -> Self
    where
        F: Fn(&Address) -> bool + Clone + Send + 'static,
        R: Fn(&Address) -> f64 + Clone + Send + 'static,
    {
        let nb_workers: usize = num_cpus::get() - 2;
        let (addr_sender, addr_receiver) = channel::bounded(CHANNEL_SIZES);
        let (hash_sender, hash_receiver) = channel::bounded(CHANNEL_SIZES);

        // --- Init worker threads

        for _ in 0..nb_workers {
            let addr_receiver = addr_receiver.clone();
            let hash_sender = hash_sender.clone();
            let filter = filter.clone();
            let ranking = ranking.clone();

            thread::spawn(move || {
                for address in addr_receiver.into_iter().filter(filter) {
                    let rank = ranking(&address);
                    let hashes: Vec<_> = hash_address(&address).collect();

                    hash_sender
                        .send((address, rank, hashes))
                        .expect("failed sending hashes: channel may have closed too early");
                }
            });
        }

        // --- Init writter thread

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

        Self {
            addr_sender,
            writter_thread,
        }
    }
}

impl Drop for DbInserter {
    fn drop(&mut self) {
        // Close sender channel, this will end writter threads
        let (closed_sender, _) = channel::unbounded();
        std::mem::replace(&mut self.addr_sender, closed_sender);

        // Wait for writter thread to finish writting
        let writter_thread = std::mem::replace(&mut self.writter_thread, thread::spawn(|| ()));
        writter_thread
            .join()
            .expect("failed to join writter thread");
    }
}

impl importer_tools::CompatibleDB for DbInserter {
    fn flush(&mut self) {}

    fn insert(&mut self, addr: Address) {
        self.addr_sender
            .send(addr)
            .expect("failed sending address: channel may have closed too early");
    }

    fn get_nb_addrs_by_cities(&self) -> Vec<(String, i64)> {
        // TODO
        Vec::new()
    }

    fn get_nb_addresses(&self) -> i64 {
        // TODO
        0
    }

    fn get_nb_errors(&self) -> i64 {
        0
    }

    fn get_nb_by_errors_kind(&self) -> Vec<(String, i64)> {
        Vec::new()
    }
}
