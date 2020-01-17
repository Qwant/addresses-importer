use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use rpostal;
use rusqlite::{Connection, ToSql, NO_PARAMS};

use crate::address::Address;

pub struct Dedupe {
    output_conn: Connection,
    rpostal_core: rpostal::Core,
}

impl Dedupe {
    pub fn new(output_conn: &PathBuf) -> rusqlite::Result<Self> {
        let output_conn = Connection::open(&output_conn)?;

        output_conn.execute(
            "CREATE TABLE IF NOT EXISTS addresses(
                lat         REAL NOT NULL,
                lon         REAL NOT NULL,
                number      TEXT,
                street      TEXT NOT NULL,
                unit        TEXT,
                city        TEXT,
                district    TEXT,
                region      TEXT,
                postcode    TEXT,
                PRIMARY KEY (lat, lon, number, street, city)
            )",
            NO_PARAMS,
        )?;

        output_conn.execute(
            "CREATE TABLE IF NOT EXISTS addresses_hashes(
                address     INTEGER NOT NULL,
                hash        INTEGER NOT NULL
            )",
            NO_PARAMS,
        )?;

        Ok(Self {
            output_conn,
            rpostal_core: rpostal::Core::setup().expect("failed to init libpostal"),
        })
    }

    pub fn load_addresses(&self, addresses: impl Iterator<Item = Address>) -> rusqlite::Result<()> {
        // Init libpostal classifier
        let rpostal_classifier = self
            .rpostal_core
            .setup_language_classifier()
            .expect("failed loading langage classifier");

        // Prepare output database
        let mut output_address_stmt = self.output_conn.prepare(
            "INSERT INTO addresses(
                lat,
                lon,
                number,
                street,
                unit,
                city,
                district,
                region,
                postcode
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);",
        )?;

        let mut output_hash_stmt = self
            .output_conn
            .prepare("INSERT INTO addresses_hashes(address, hash) VALUES (?1, ?2);")?;

        // Insert addresses
        let mut last_display_index = 0;
        let mut last_display_instant = Instant::now();
        self.output_conn.execute_batch("BEGIN TRANSACTION;")?;

        for (index, address) in addresses.enumerate() {
            output_address_stmt.execute(&[
                &address.lat as &dyn ToSql,
                &address.lon,
                &address.number,
                &address.street,
                &address.unit,
                &address.city,
                &address.district,
                &address.region,
                &address.postcode,
            ])?;

            let address_rowid = self.output_conn.last_insert_rowid();
            let hashes = hash_address(&rpostal_classifier, &address);

            for hash in hashes {
                output_hash_stmt.execute(&[&address_rowid, &(hash as i64) as &dyn ToSql])?;
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

        self.output_conn.execute_batch("COMMIT TRANSACTION;")?;
        Ok(())
    }

    pub fn remove_duplicate(&self) -> rusqlite::Result<()> {
        eprintln!("Query hash collisions");

        let mut stmt = self.output_conn.prepare(
            "
                SELECT DISTINCT addr_1.rowid AS id_1, addr_2.rowid AS id_2
                FROM addresses AS addr_1
                JOIN addresses AS addr_2
                JOIN addresses_hashes AS hash_1 ON addr_1.rowid = hash_1.address
                JOIN addresses_hashes AS hash_2 ON addr_2.rowid = hash_2.address
                WHERE
                    id_1 < id_2 AND hash_1.hash = hash_2.hash;
            ",
        )?;

        let feasible_duplicates = stmt
            .query_map(NO_PARAMS, |row| Ok((row.get("id_1")?, row.get("id_2")?)))?
            .map(|pair| {
                pair.unwrap_or_else(|e| panic!("failed reading feasible duplicate: {:?}", e))
            });

        for dupe in feasible_duplicates {
            let (x, y): (i64, i64) = dupe;
            println!("{}, {}", x, y);
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
