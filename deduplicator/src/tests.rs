extern crate tempdir;

use std::cmp::Ordering;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;

use importer_tools::Address;
use rusqlite::Connection;
use tempdir::TempDir;

use crate::dedupe::Dedupe;
use crate::utils::{iter_addresses_from_stmt, iter_addresses_stmt};

fn load_addresses_from_db(conn: &Connection, table: &str) -> rusqlite::Result<Vec<Address>> {
    let mut res = Vec::new();
    let mut stmt = iter_addresses_stmt(conn, table)?;
    let iter = iter_addresses_from_stmt(&mut stmt)?;

    for address in iter {
        res.push(address?);
    }

    Ok(res)
}

fn load_db(path: &PathBuf) -> rusqlite::Result<Connection> {
    let mut sql_buff = String::new();
    let mut file = File::open(path).expect("failed to load test database");
    file.read_to_string(&mut sql_buff)
        .expect("failed reading file");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch(&sql_buff)?;
    Ok(conn)
}

/// Check that no item is removed from a database without duplicates.
#[test]
fn database_complete() -> rusqlite::Result<()> {
    let tmp_dir = TempDir::new("output").unwrap();
    let output_path = tmp_dir.path().join("addresses.db");
    let input_path = PathBuf::from("src/data/tests/db_1.sql");

    // Read input database
    let mut input_addresses = load_addresses_from_db(&load_db(&input_path)?, "addresses")?;
    let mut dedupe = Dedupe::new(tmp_dir.path().join("addresses.db"))?;
    dedupe.load_addresses(input_addresses.clone().into_iter(), |_| 1.)?;
    dedupe.compute_duplicates()?;
    dedupe.apply_and_clean()?;

    // Read output database
    let mut output_addresses =
        load_addresses_from_db(&Connection::open(&output_path)?, "addresses")?;

    // Compare results
    input_addresses
        .sort_by(|addr_1, addr_2| addr_1.partial_cmp(&addr_2).unwrap_or(Ordering::Equal));
    output_addresses
        .sort_by(|addr_1, addr_2| addr_1.partial_cmp(&addr_2).unwrap_or(Ordering::Equal));

    assert_eq!(input_addresses.len(), output_addresses.len());
    assert!(input_addresses
        .into_iter()
        .zip(output_addresses.into_iter())
        .all(|(addr_1, addr_2)| addr_1 == addr_2));

    Ok(())
}

/// Check that all perfect duplicates are removed from the database.
#[test]
fn remove_exact_duplicates() -> rusqlite::Result<()> {
    let tmp_dir = TempDir::new("output").unwrap();
    let output_path = tmp_dir.path().join("addresses.db");
    let input_path = PathBuf::from("src/data/tests/db_1.sql");

    // Read input database
    let mut input_addresses = load_addresses_from_db(&load_db(&input_path)?, "addresses")?;
    let mut dedupe = Dedupe::new(tmp_dir.path().join("addresses.db"))?;
    dedupe.load_addresses(input_addresses.clone().into_iter(), |_| 1.)?;
    dedupe.load_addresses(input_addresses.clone().into_iter(), |_| 1.)?;
    dedupe.compute_duplicates()?;
    dedupe.apply_and_clean()?;

    // Read output database
    let mut output_addresses =
        load_addresses_from_db(&Connection::open(&output_path)?, "addresses")?;

    // Compare results
    input_addresses
        .sort_by(|addr_1, addr_2| addr_1.partial_cmp(&addr_2).unwrap_or(Ordering::Equal));
    output_addresses
        .sort_by(|addr_1, addr_2| addr_1.partial_cmp(&addr_2).unwrap_or(Ordering::Equal));

    assert_eq!(input_addresses.len(), output_addresses.len());
    assert!(input_addresses
        .into_iter()
        .zip(output_addresses.into_iter())
        .all(|(addr_1, addr_2)| addr_1 == addr_2));

    Ok(())
}
