use std::convert::{TryFrom, TryInto};
use std::ffi::CString;
use std::path::PathBuf;

use crate::dedupe::Dedupe;

use importer_tools::{Address, CompatibleDB};
use libsqlite3_sys::ErrorCode::ConstraintViolation;
use prog_rs::prelude::*;
use rusqlite::{Connection, Statement, NO_PARAMS};

pub fn postal_repr(address: &Address) -> Vec<rpostal::Address> {
    [
        ("house_number", &address.number),
        ("road", &address.street),
        ("unit", &address.unit),
        ("city", &address.city),
        ("state_district", &address.district),
        ("country_region", &address.region),
        ("postcode", &address.postcode),
    ]
    .into_iter()
    .filter_map(|(key, val)| {
        val.as_ref().map(|val| rpostal::Address {
            label: CString::new(key.as_bytes()).unwrap(),
            value: CString::new(val.as_bytes()).unwrap(),
        })
    })
    .collect()
}

pub fn is_constraint_violation_error(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(
            libsqlite3_sys::Error {
                code: ConstraintViolation,
                extended_code: _,
            },
            _,
        ) => true,
        _ => false,
    }
}

pub fn iter_addresses_stmt<'c>(
    conn: &'c Connection,
    table: &str,
) -> rusqlite::Result<Statement<'c>> {
    conn.prepare(&format!("select * from {}", table))
}

pub fn iter_addresses_from_stmt<'s>(
    stmt: &'s mut Statement,
) -> rusqlite::Result<impl Iterator<Item = rusqlite::Result<Address>> + 's> {
    stmt.query_map(NO_PARAMS, |row| row.try_into())
}

pub fn load_from_sqlite<F, R>(
    deduplication: &mut Dedupe,
    path: PathBuf,
    filter: F,
    ranking: R,
) -> rusqlite::Result<()>
where
    F: Fn(&Address) -> bool + Clone + Send + 'static,
    R: Fn(&Address) -> f64 + Clone + Send + 'static,
{
    let input_conn = Connection::open(&path)?;
    let nb_addresses = usize::try_from(input_conn.query_row(
        "SELECT COUNT(*) FROM addresses;",
        NO_PARAMS,
        |row| row.get(0).map(|x: isize| x),
    )?)
    .expect("failed to count number of addresses");

    // Query list of addresses
    let mut stmt = iter_addresses_stmt(&input_conn, "addresses")?;
    let addresses = iter_addresses_from_stmt(&mut stmt)?
        .progress()
        .with_iter_size(nb_addresses)
        .with_prefix(format!("{:<45}", format!("{:?}", path)))
        .filter_map(|addr| {
            addr.map_err(|e| eprintln!("failed to read address from DB: {}", e))
                .ok()
        });

    // Insert addresses
    let mut inserter = deduplication.get_db_inserter(filter, ranking)?;

    for address in addresses {
        inserter.insert(address);
    }

    Ok(())
}
