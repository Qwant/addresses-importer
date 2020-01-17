extern crate rpostal;
extern crate rusqlite;
extern crate structopt;

mod address;
mod address_hasher;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use rusqlite::{Connection, ToSql, NO_PARAMS};
use structopt::StructOpt;

use address::Address;
use address_hasher::AddressHasher;

const DEBUG_DELAY: u64 = 10;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "deduplicator",
    about = "Deduplicate addresses from several sources."
)]
struct Params {
    /// Path to data from various sources.
    #[structopt(short, long)]
    sources: Vec<PathBuf>,

    /// Path to output database.
    #[structopt(short, long, default_value = "addresses.db")]
    output: PathBuf,
}

fn import_source(output_conn: &Connection, input_path: &PathBuf) -> rusqlite::Result<()> {
    // Prepare output database

    let mut output_address_stmt = output_conn.prepare(
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

    let mut output_hash_stmt =
        output_conn.prepare("INSERT INTO addresses_hashes(address, hash) VALUES (?1, ?2);")?;

    // Fetch addresses from file
    let input_conn = Connection::open(input_path)?;
    let mut stmt = input_conn.prepare("SELECT * FROM addresses")?;
    let rows = stmt.query_map(NO_PARAMS, |row| {
        Ok(Address {
            lat: row.get("lat")?,
            lon: row.get("lon")?,
            number: row.get("number")?,
            street: row.get("street")?,
            unit: row.get("unit")?,
            city: row.get("city")?,
            district: row.get("district")?,
            region: row.get("region")?,
            postcode: row.get("postcode")?,
        })
    })?;

    // Init address hasher
    let rpostal_core = rpostal::Core::setup().expect("failed loading rpostal core");
    let rpostal_classifier = rpostal_core
        .setup_language_classifier()
        .expect("failed loading rpostal langage classifier");
    let address_hasher = AddressHasher::new(&rpostal_classifier);

    // Insert addresses
    let mut last_display_index = 0;
    let mut last_display_instant = Instant::now();
    output_conn.execute_batch("BEGIN TRANSACTION;")?;

    for (index, address) in rows.enumerate() {
        let address = address?;

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

        let address_rowid = output_conn.last_insert_rowid();
        let hashes = address_hasher.hash_address(&address);

        for hash in hashes {
            output_hash_stmt.execute(&[&address_rowid, &(hash as i64) as &dyn ToSql])?;
        }

        if last_display_instant.elapsed() >= Duration::from_secs(DEBUG_DELAY) {
            eprintln!(
                "Processed {} addresses in {:?}",
                index - last_display_index,
                last_display_instant.elapsed()
            );

            last_display_instant = Instant::now();
            last_display_index = index;
        }
    }

    output_conn.execute_batch("COMMIT TRANSACTION;")?;
    Ok(())
}

fn main() {
    let params = Params::from_args();

    // Prepare output database
    let output_conn = Connection::open(&params.output)
        .unwrap_or_else(|e| panic!("failed to open output database: {:?}", e));

    output_conn
        .execute(
            "CREATE TABLE IF NOT EXISTS addresses(
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                number TEXT,
                street TEXT NOT NULL,
                unit TEXT,
                city TEXT,
                district TEXT,
                region TEXT,
                postcode TEXT,
                PRIMARY KEY (lat, lon, number, street, city)
            )",
            NO_PARAMS,
        )
        .unwrap_or_else(|e| panic!("failed to create table `addresses`: {}", e));

    output_conn
        .execute(
            "CREATE TABLE IF NOT EXISTS addresses_hashes(
                address     INTEGER NOT NULL,
                hash        INTEGER NOT NULL
            )",
            NO_PARAMS,
        )
        .unwrap_or_else(|e| panic!("failed to create table `_addresses_hashes`: {}", e));

    for path in &params.sources {
        import_source(&output_conn, path).unwrap();
    }

    println!("{:?}", params);
}
