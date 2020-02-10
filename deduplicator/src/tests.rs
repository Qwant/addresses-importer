extern crate tempdir;

use std::convert::TryInto;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;

use rusqlite::{Connection, NO_PARAMS};
use tempdir::TempDir;
use tools::Address;
use tools::CompatibleDB;

use crate::deduplicator::Deduplicator;

const DB_NO_DUPES: &str = "data/tests/no_dupes.sql";
const DB_WITH_DUPES: &str = "data/tests/with_dupes.sql";

/// Create an SQLite database from Sql dump
fn load_dump(path: &PathBuf) -> rusqlite::Result<Connection> {
    let mut sql_buff = String::new();
    let mut file = File::open(path).expect("failed to load test database");
    file.read_to_string(&mut sql_buff)
        .expect("failed reading file");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch(&sql_buff)?;
    Ok(conn)
}

/// Load addresses from SQLite database
fn load_addresses_from_db(conn: &Connection) -> rusqlite::Result<Vec<Address>> {
    let mut res = Vec::new();
    let mut stmt = conn.prepare("SELECT * FROM addresses;")?;
    let iter = stmt.query_map(NO_PARAMS, |row| row.try_into())?;

    for address in iter {
        res.push(address?);
    }

    Ok(res)
}

/// Insert addresses in the deduplicator
fn insert_addresses(
    dedupe: &mut Deduplicator,
    addresses: impl IntoIterator<Item = Address>,
) -> rusqlite::Result<()> {
    let mut inserter = dedupe.get_db_inserter(|_| true, |_| 1.)?;

    for address in addresses {
        inserter.insert(address);
    }

    Ok(())
}

fn assert_same_addresses(mut addresses_1: Vec<Address>, mut addresses_2: Vec<Address>) {
    addresses_1.sort_by(|addr_1, addr_2| addr_1.partial_cmp(&addr_2).unwrap());
    addresses_2.sort_by(|addr_1, addr_2| addr_1.partial_cmp(&addr_2).unwrap());

    assert_eq!(addresses_1.len(), addresses_2.len());

    for (addr_1, addr_2) in addresses_1.into_iter().zip(addresses_2) {
        assert_eq!(addr_1, addr_2);
    }
}

/// Check that no item is removed from a database without duplicates.
#[test]
fn database_complete() -> rusqlite::Result<()> {
    let tmp_dir = TempDir::new("output").unwrap();
    let output_path = tmp_dir.path().join("addresses.db");

    // Read input database
    let input_addresses = load_addresses_from_db(&load_dump(&DB_NO_DUPES.into())?)?;
    let mut dedupe = Deduplicator::new(tmp_dir.path().join("addresses.db"), None)?;
    insert_addresses(&mut dedupe, input_addresses.clone())?;
    dedupe.compute_duplicates()?;
    dedupe.apply_and_clean(false)?;

    // Read output database
    let output_addresses = load_addresses_from_db(&Connection::open(&output_path)?)?;

    // Compare results
    assert_same_addresses(input_addresses, output_addresses);
    Ok(())
}

/// Check that all perfect duplicates are removed from the database.
#[test]
fn remove_exact_duplicates() -> rusqlite::Result<()> {
    let tmp_dir = TempDir::new("output").unwrap();
    let output_path = tmp_dir.path().join("addresses.db");

    // Read input database
    let input_addresses = load_addresses_from_db(&load_dump(&DB_NO_DUPES.into())?)?;
    let mut dedupe = Deduplicator::new(tmp_dir.path().join("addresses.db"), None)?;

    // Insert all addresses 10 times
    for _ in 0..10 {
        insert_addresses(&mut dedupe, input_addresses.clone())?;
    }

    dedupe.compute_duplicates()?;
    dedupe.apply_and_clean(false)?;

    // Read output database
    let output_addresses = load_addresses_from_db(&Connection::open(&output_path)?)?;

    // Compare results
    assert_same_addresses(input_addresses, output_addresses);
    Ok(())
}

/// Check that all non-trivial duplicates are removed.
#[test]
fn remove_close_duplicates() -> rusqlite::Result<()> {
    let tmp_dir = TempDir::new("output").unwrap();
    let output_path = tmp_dir.path().join("addresses.db");

    // Read input database
    let input_addresses = load_addresses_from_db(&load_dump(&DB_WITH_DUPES.into())?)?;
    let mut dedupe = Deduplicator::new(tmp_dir.path().join("addresses.db"), None)?;
    insert_addresses(&mut dedupe, input_addresses)?;
    dedupe.compute_duplicates()?;
    dedupe.apply_and_clean(false)?;

    // Read output database
    let output_addresses = load_addresses_from_db(&Connection::open(&output_path)?)?;
    for addr in &output_addresses {
        println!("{:?}", &addr);
    }
    assert_eq!(output_addresses.len(), 10);
    Ok(())
}
