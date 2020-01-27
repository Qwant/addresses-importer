use std::convert::TryFrom;
use std::path::PathBuf;

use crate::address::Address;
use crate::dedupe::Dedupe;

use libsqlite3_sys::ErrorCode::ConstraintViolation;
use prog_rs::prelude::*;
use rusqlite::{Connection, NO_PARAMS};

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

pub fn load_from_sqlite<F, R>(
    deduplication: &mut Dedupe,
    path: PathBuf,
    filter: F,
    ranking: R,
) -> rusqlite::Result<()>
where
    F: Fn(&Address) -> bool,
    R: Fn(&Address) -> f64 + Clone + Send + 'static,
{
    let input_conn = Connection::open(&path)?;
    let nb_addresses = usize::try_from(input_conn.query_row(
        "SELECT COUNT(*) FROM addresses;",
        NO_PARAMS,
        |row| row.get(0).map(|x: isize| x),
    )?)
    .expect("failed to count number of addresses");

    let mut stmt = input_conn.prepare("SELECT * FROM addresses")?;
    let addresses = stmt
        .query_map(NO_PARAMS, |row| Address::from_sqlite_row(&row))?
        .progress()
        .with_iter_size(nb_addresses)
        .with_prefix(format!("{:<45}", format!("{:?}", path)))
        .filter_map(|addr| {
            addr.map_err(|e| eprintln!("failed to read address from DB: {}", e))
                .ok()
        })
        .filter(filter);

    deduplication.load_addresses(addresses, ranking)
}

