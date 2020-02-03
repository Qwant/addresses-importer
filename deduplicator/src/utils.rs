use std::convert::{TryFrom, TryInto};
use std::ffi::CString;
use std::path::PathBuf;

use crate::deduplicator::Deduplicator;

use importer_tools::{Address, CompatibleDB};
use libsqlite3_sys::ErrorCode::ConstraintViolation;
use prog_rs::prelude::*;
use rusqlite::{Connection, NO_PARAMS};

pub fn field_compare<T>(
    field1: &Option<T>,
    field2: &Option<T>,
    compare: impl Fn(&T, &T) -> bool,
) -> bool {
    match (field1.as_ref(), field2.as_ref()) {
        (Some(field1), Some(field2)) => compare(field1, field2),
        _ => false,
    }
}

pub fn opt_field_compare<T>(
    field1: &Option<T>,
    field2: &Option<T>,
    compare: impl Fn(&T, &T) -> bool,
) -> bool {
    match (field1.as_ref(), field2.as_ref()) {
        (None, None) => true,
        (Some(field1), Some(field2)) => compare(field1, field2),
        _ => false,
    }
}

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
    .iter()
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
                ..
            },
            _,
        ) => true,
        _ => false,
    }
}

pub fn load_from_sqlite<F, R>(
    deduplication: &mut Deduplicator,
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
    let mut stmt = input_conn.prepare("SELECT * FROM addresses;")?;
    let addresses = stmt
        .query_map(NO_PARAMS, |row| row.try_into())?
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
