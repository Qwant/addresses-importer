use std::cmp::max;
use std::io::{stderr, Write};
use std::mem::drop;
use std::path::PathBuf;
use std::thread;

use crossbeam_channel as channel;
use importer_openaddresses::OpenAddress;
use itertools::Itertools;
use prog_rs::prelude::*;
use rusqlite::DropBehavior;
use tools::Address;

use crate::db_hashes::{DbHashes, HashIterItem};
use crate::dedupe::{hash_address, is_duplicate};
use crate::utils::is_constraint_violation_error;

const CHANNELS_SIZE: usize = 100_000;

pub struct Deduplicator {
    db: DbHashes,
}

impl Deduplicator {
    pub fn new(output_path: PathBuf, cache_size: Option<u32>) -> rusqlite::Result<Self> {
        Ok(Self {
            db: DbHashes::new(output_path, cache_size)?,
        })
    }

    pub fn get_db_inserter<F, R>(
        &mut self,
        filter: F,
        ranking: R,
    ) -> rusqlite::Result<DbInserter<F, R>>
    where
        F: Fn(&Address) -> bool + Clone + Send + 'static,
        R: Fn(&Address) -> f64 + Clone + Send + 'static,
    {
        Ok(DbInserter::new(&self.db, filter, ranking)?)
    }

    pub fn compute_duplicates(&mut self) -> rusqlite::Result<()> {
        teprintln!("Build index on hashes");
        self.db.create_hashes_index()?;

        // --- Query collisions from DB
        let count_addresses_before = self.db.count_addresses()?;
        let count_hashes = self.db.count_hashes()?;

        teprintln!(
            "Compute hash collisions ({} addresses, {} hashes)",
            count_addresses_before,
            count_hashes
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
        // [    del_receiver    ] writer thread

        let nb_workers = max(3, num_cpus::get()) - 2;
        let (col_sender, col_receiver) = channel::bounded::<Vec<HashIterItem>>(CHANNELS_SIZE);
        let (del_sender, del_receiver) = channel::bounded(CHANNELS_SIZE);

        // --- Init worker threads

        for _ in 0..nb_workers {
            let col_receiver = col_receiver.clone();
            let del_sender = del_sender.clone();

            thread::spawn(move || {
                for mut pack in col_receiver {
                    if pack.len() > 5000 {
                        // In practice this should not happen often, however in the case where this
                        // issue is raised, it would be necessary to implement a specific way of
                        // handling big packs (for example by computing more accurate hashes in
                        // RAM).
                        //
                        // Current behaviour is to ignore these large packs to avoid extremely long
                        // computation time, but dump the content of the pack into stderr to ease
                        // investigation.
                        teprintln!(
                            r"/!\ Performance danger: skipping pack of length {}",
                            pack.len()
                        );
                        teprintln!("Here are the first 10 addresses of the pack:");

                        {
                            let mut stream = stderr();
                            let mut writer = csv::Writer::from_writer(&mut stream);

                            for item in pack.into_iter().take(10) {
                                writer.serialize(OpenAddress::from(item.address)).ok();
                            }

                            writer.flush().expect("failed to flush CSV dump");
                        }

                        continue;
                    }

                    // Place items we want to keep the most (ie. with greater rank) at the begining
                    // of the array.
                    pack.sort_unstable_by(|item_1, item_2| {
                        (item_1.rank, item_1.id)
                            .partial_cmp(&(item_2.rank, item_2.id))
                            .unwrap_or_else(|| item_1.id.cmp(&item_2.id))
                            .reverse()
                    });

                    // Keep track of addresses that will not be removed, each address will only be
                    // compared with "first" element of other equivalence classes.
                    let mut kept_items: Vec<_> = pack.first().into_iter().collect();

                    for item in &pack[1..] {
                        let item_is_duplicate = kept_items
                            .iter()
                            .any(|kept| is_duplicate(&item.address, &kept.address));

                        if item_is_duplicate {
                            del_sender.send(item.id).expect(
                                "failed sending id to delete: channel may have closed to early",
                            );
                        } else {
                            kept_items.push(item);
                        }
                    }
                }
            });
        }

        // Drop channels that were cloned before being sent
        drop(col_receiver);
        drop(del_sender);

        // --- Init writer thread

        let mut conn_insert = self.db.get_conn()?;

        let writer_thread = thread::spawn(move || {
            let mut tran_insert = conn_insert
                .transaction()
                .expect("failed to init transaction");
            tran_insert.set_drop_behavior(DropBehavior::Commit);
            let mut inserter =
                DbHashes::get_inserter(&mut tran_insert).expect("failed to init inserter");

            // Collect the list of addresses to remove
            let to_delete: std::collections::HashSet<_> = del_receiver.iter().collect();

            for id in to_delete {
                match inserter.insert_to_delete(id) {
                    Err(err) if !is_constraint_violation_error(&err) => {
                        teprintln!("Failed to insert id to delete in the database: {}", err)
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
            .with_prefix("Filter colisions")
            .with_output_stream(prog_rs::OutputStream::StdErr)
            .filter_map(|item| {
                item.map_err(|err| teprintln!("Failed retrieving hash: {}", err))
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
        writer_thread.join().expect("failed joining writing thread");
        Ok(())
    }

    pub fn apply_and_clean(&self, keep_construction_tables: bool) -> rusqlite::Result<()> {
        let count_to_delete = self.db.count_to_delete()?;
        teprint!("Deleting {} addresses ...\r", count_to_delete);

        self.db.apply_addresses_to_delete()?;

        let count_remain = self.db.count_addresses()?;
        teprintln!(
            "Deleting {} addresses ... {} remain",
            count_to_delete,
            count_remain
        );

        if !keep_construction_tables {
            teprintln!("Cleaning database");
            self.db.cleanup_database()?;

            teprintln!("Vacuum database");
            self.db.vacuum()?;
        }

        Ok(())
    }

    pub fn openaddresses_dump<W: Write>(&self, mut stream: W) -> rusqlite::Result<()> {
        // Fetch addresses
        let conn = self.db.get_conn()?;
        let mut addresses = DbHashes::get_addresses(&conn)?;

        // Dump into stream
        {
            let mut writer = csv::Writer::from_writer(&mut stream);

            for address in addresses.iter()? {
                writer
                    .serialize(OpenAddress::from(address?))
                    .unwrap_or_else(|err| teprintln!("Failed to write address: {}", err));
            }

            writer.flush().expect("failed to flush CSV dump");
        }

        stream.flush().unwrap();
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
// [     hash_receiver    ] writer thread

pub struct DbInserter<'db, F, R>
where
    F: Fn(&Address) -> bool + Clone + Send + 'static,
    R: Fn(&Address) -> f64 + Clone + Send + 'static,
{
    db: &'db DbHashes,
    addr_sender: Option<channel::Sender<Address>>,
    writer_thread: Option<thread::JoinHandle<()>>,
    filter: F,
    ranking: R,
}

impl<'db, F, R> DbInserter<'db, F, R>
where
    F: Fn(&Address) -> bool + Clone + Send + 'static,
    R: Fn(&Address) -> f64 + Clone + Send + 'static,
{
    pub fn new(db: &'db DbHashes, filter: F, ranking: R) -> rusqlite::Result<Self> {
        let mut inserter = Self {
            db,
            addr_sender: None,
            writer_thread: None,
            filter,
            ranking,
        };
        inserter.start_transaction()?;
        Ok(inserter)
    }

    fn start_transaction(&mut self) -> rusqlite::Result<()> {
        // Ensure that previous transactions was commited and channels are empty.
        self.stop_transaction();

        // --- Create new channels for new threads

        let nb_workers = max(3, num_cpus::get()) - 2;
        let (addr_sender, addr_receiver) = channel::bounded(CHANNELS_SIZE);
        let (hash_sender, hash_receiver) = channel::bounded(CHANNELS_SIZE);

        // --- Init worker threads

        for _ in 0..nb_workers {
            let addr_receiver = addr_receiver.clone();
            let hash_sender = hash_sender.clone();
            let filter = self.filter.clone();
            let ranking = self.ranking.clone();

            thread::spawn(move || {
                for address in addr_receiver.into_iter().filter(filter) {
                    let rank = ranking(&address);
                    let hashes: Vec<_> = hash_address(&address).collect();

                    if hashes.is_empty() {
                        teprintln!("Ignoring an address that can't be hashed: {:?}", address);
                        continue;
                    }

                    hash_sender
                        .send((address, rank, hashes))
                        .expect("failed sending hashes: channel may have closed too early");
                }
            });
        }

        // --- Init writer thread

        let mut conn = self.db.get_conn()?;
        self.writer_thread = Some(thread::spawn(move || {
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
                                        teprintln!("Failed inserting hash: {}", err);
                                    }
                                })
                                .ok();
                        }
                    }
                    Err(err) if !is_constraint_violation_error(&err) => {
                        teprintln!("Failed inserting address: {}", err);
                    }
                    _ => (),
                }
            }
        }));

        self.addr_sender = Some(addr_sender);
        Ok(())
    }

    /// Commit and stop transaction, this means that you can't call `self.insert` until
    /// `self.start_transaction` is called.
    fn stop_transaction(&mut self) {
        // Close sender channel, this will end writer threads
        self.addr_sender = None;

        // Wait for writer thread to finish writing if any
        if let Some(writer_thread) = std::mem::replace(&mut self.writer_thread, None) {
            writer_thread.join().expect("failed to join writer thread");
        }
    }

    /// By default DbInserter applies all its actions in a single transaction handled by the worker
    /// thread. The issue is that this locks database and it is not even possible to execute
    /// read only queries.
    ///
    /// This function will close channels (which will stop all threads), execute an action and
    /// restart everything.
    fn borrow_db<A, T>(&mut self, action: A) -> rusqlite::Result<T>
    where
        A: FnOnce(&DbHashes) -> rusqlite::Result<T>,
    {
        self.stop_transaction();
        let result = action(self.db);
        self.start_transaction()?;
        result
    }
}

impl<'db, F, R> Drop for DbInserter<'db, F, R>
where
    F: Fn(&Address) -> bool + Clone + Send + 'static,
    R: Fn(&Address) -> f64 + Clone + Send + 'static,
{
    fn drop(&mut self) {
        self.stop_transaction()
    }
}

impl<'db, F, R> tools::CompatibleDB for DbInserter<'db, F, R>
where
    F: Fn(&Address) -> bool + Clone + Send + 'static,
    R: Fn(&Address) -> f64 + Clone + Send + 'static,
{
    fn flush(&mut self) {
        // Flush doesn't realy make sense while the database is locked.
    }

    fn insert(&mut self, addr: Address) {
        if addr.number.as_ref().map(|num| num == "S/N").unwrap_or(true) {
            // house number is not specified
            return;
        }

        self.addr_sender
            .as_ref()
            .expect("failed sending address: transaction is closed")
            .send(addr)
            .expect("failed sending address: channel may have closed too early")
    }

    fn get_nb_cities(&mut self) -> i64 {
        self.borrow_db(|db| db.count_cities())
            .map_err(|err| eprintln!("Failed counting cities: '{}'", err))
            .unwrap_or(0)
    }

    fn get_nb_addresses(&mut self) -> i64 {
        self.borrow_db(|db| db.count_addresses())
            .map_err(|err| eprintln!("Failed counting addresses: '{}'", err))
            .unwrap_or(0)
    }

    fn get_address(&mut self, housenumber: i32, street: &str) -> Vec<Address> {
        self.borrow_db(|db| db.get_addresses_by_street(housenumber, street))
            .map_err(|err| eprintln!("Error while retrieving addresses by street: '{}'", err))
            .unwrap_or_default()
    }

    // Current implementation for the deduplication actually doesn't log errors.
    fn get_nb_errors(&mut self) -> i64 {
        0
    }

    fn get_nb_by_errors_kind(&mut self) -> Vec<(String, i64)> {
        Vec::new()
    }
}
