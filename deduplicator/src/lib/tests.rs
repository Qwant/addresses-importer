extern crate tempdir;

use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;

use rusqlite::Connection;
use tempdir::TempDir;
use tools::{Address, CompatibleDB, OpenAddressLegacy};

use crate::deduplicator::{DedupeConfig, Deduplicator};
use crate::utils::partition;

const DB_NO_DUPES: &str = "data/tests/no_dupes.sql";
const DB_WITH_DUPES: &str = "data/tests/with_dupes.sql";

/// Create an SQLite database from Sql dump
fn load_dump(path: PathBuf) -> rusqlite::Result<Connection> {
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
    let iter = stmt.query_map([], |row| row.try_into())?;

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

fn assert_same_addresses<A: Into<Address>, B: Into<Address>>(
    addresses_1: Vec<A>,
    addresses_2: Vec<B>,
) {
    let mut addresses_1: Vec<Address> = addresses_1.into_iter().map(Into::into).collect();
    let mut addresses_2: Vec<Address> = addresses_2.into_iter().map(Into::into).collect();
    addresses_1.sort_by(|addr_1, addr_2| addr_1.partial_cmp(addr_2).unwrap());
    addresses_2.sort_by(|addr_1, addr_2| addr_1.partial_cmp(addr_2).unwrap());
    assert_eq!(addresses_1, addresses_2);
}

/// Check that no item is removed from a database without duplicates.
#[test]
fn database_complete() -> rusqlite::Result<()> {
    let tmp_dir = TempDir::new("output").unwrap();
    let output_path = tmp_dir.path().join("addresses.db");

    // Read input database
    let input_addresses = load_addresses_from_db(&load_dump(DB_NO_DUPES.into())?)?;
    let mut dedupe = Deduplicator::new(
        tmp_dir.path().join("addresses.db"),
        DedupeConfig::default(),
        None,
    )?;
    insert_addresses(&mut dedupe, input_addresses.clone())?;
    dedupe.compute_duplicates()?;
    dedupe.apply_deletions()?;

    // Read output database
    let output_addresses = load_addresses_from_db(&Connection::open(output_path)?)?;

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
    let input_addresses = load_addresses_from_db(&load_dump(DB_NO_DUPES.into())?)?;
    let mut dedupe = Deduplicator::new(
        tmp_dir.path().join("addresses.db"),
        DedupeConfig::default(),
        None,
    )?;

    // Insert all addresses 10 times
    for _ in 0..10 {
        insert_addresses(&mut dedupe, input_addresses.clone())?;
    }

    dedupe.compute_duplicates()?;
    dedupe.apply_deletions()?;

    // Read output database
    let output_addresses = load_addresses_from_db(&Connection::open(output_path)?)?;

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
    let input_addresses = load_addresses_from_db(&load_dump(DB_WITH_DUPES.into())?)?;
    let mut dedupe = Deduplicator::new(
        tmp_dir.path().join("addresses.db"),
        DedupeConfig::default(),
        None,
    )?;
    insert_addresses(&mut dedupe, input_addresses)?;
    dedupe.compute_duplicates()?;
    dedupe.apply_deletions()?;

    // Read output database
    let output_addresses = load_addresses_from_db(&Connection::open(output_path)?)?;
    assert_eq!(output_addresses.len(), 10);
    Ok(())
}

/// Check that no data is altered while writting into a CSV dump.
#[test]
fn csv_is_complete() -> rusqlite::Result<()> {
    let tmp_dir = TempDir::new("output").unwrap();
    let output_path = tmp_dir.path().join("addresses.db");
    let output_csv_path = tmp_dir.path().join("addresses.csv.gz");

    // Read input database
    let input_addresses = load_addresses_from_db(&load_dump(DB_NO_DUPES.into())?)?;
    let mut dedupe = Deduplicator::new(
        tmp_dir.path().join("addresses.db"),
        DedupeConfig::default(),
        None,
    )?;
    insert_addresses(&mut dedupe, input_addresses)?;

    // Dump CSV
    let file = File::create(&output_csv_path).expect("failed to create dump file");
    dedupe.openaddresses_dump(file)?;

    // Read output database
    let output_addresses = load_addresses_from_db(&Connection::open(output_path)?)?;

    // Read output CSV
    let csv_file = File::open(&output_csv_path).unwrap();
    let mut reader = csv::Reader::from_reader(csv_file);
    let csv_addresses: Vec<OpenAddressLegacy> =
        reader.deserialize().map(|line| line.unwrap()).collect();

    // Compare results
    assert_same_addresses(output_addresses, csv_addresses);
    Ok(())
}

#[test]
fn test_partition() {
    for min_val in 0..=100 {
        for max_val in min_val..=100 {
            for nb_parts in 1..=10 {
                assert_eq!(partition(min_val..=max_val, nb_parts).count(), nb_parts);
                assert_eq!(
                    partition(min_val..=max_val, nb_parts)
                        .flatten()
                        .collect::<Vec<_>>(),
                    (min_val..=max_val).collect::<Vec<_>>()
                );
            }
        }
    }
}
