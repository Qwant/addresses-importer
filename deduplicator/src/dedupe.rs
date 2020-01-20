use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use rpostal;
use rusqlite::Connection;

use crate::address::Address;
use crate::db_hashes::DbHashes;

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
        let mut last_display_index = 0;
        let mut last_display_instant = Instant::now();
        self.db.begin_transaction()?;

        for (index, address) in addresses.enumerate() {
            let address_rowid = self.db.insert_address(&address)?;
            let hashes = hash_address(&rpostal_classifier, &address);

            for hash in hashes {
                self.db.insert_hash(address_rowid, hash as i64)?;
            }

            if last_display_instant.elapsed() >= Duration::from_secs(10) {
                eprintln!(
                    "Processed {} addresses in {:?}",
                    index - last_display_index,
                    last_display_instant.elapsed()
                );

                last_display_instant = Instant::now();
                last_display_index = index;
            }
        }

        self.db.commit_transaction()?;
        Ok(())
    }

    pub fn remove_duplicate(&self) -> rusqlite::Result<()> {
        eprintln!("Query hash collisions");

        for collision in self.db.feasible_duplicates()?.iter()? {
            let (addr_1, addr_2) = collision?;
            println!("|||||\n{:?}\n{:?}", addr_1, addr_2);
        }

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
