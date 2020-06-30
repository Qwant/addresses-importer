//! Generic utilities.

use std::borrow::Borrow;
use std::convert::{TryFrom, TryInto};
use std::ffi::CString;
use std::num::ParseIntError;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::time::Duration;

use crate::deduplicator::Deduplicator;

use libsqlite3_sys::ErrorCode::ConstraintViolation;
use prog_rs::prelude::*;
use rusqlite::{Connection, NO_PARAMS};
use tools::{Address, CompatibleDB};

/// Partition a range into several distinct partitions, given by increasing value.
///
/// # Example
///
/// ```
/// use deduplicator::utils::partition;
///
/// assert_eq!(partition(0..=99, 2).collect::<Vec<_>>(), vec![0..=49, 50..=99]);
/// ```
pub fn partition(
    range: RangeInclusive<i64>,
    nb_parts: usize,
) -> impl Iterator<Item = RangeInclusive<i64>> {
    let min = i128::from(*range.start());
    let max = i128::from(*range.end()) + 1i128;
    let nb_parts = i128::try_from(nb_parts).expect("overflow when computing partitions");

    let bounds = (0..=nb_parts).map(move |part| min + part * (max - min) / nb_parts);

    bounds.clone().zip(bounds.skip(1)).map(|(start, end)| {
        let start: i64 = start.try_into().expect("math error in partition");
        let end: i64 = end.try_into().expect("math error in partition");
        start..=(end - 1)
    })
}

/// Parse a string into a duration from a number of milliseconds.
///
/// # Example
///
/// ```
/// use deduplicator::utils::*;
/// use std::time::Duration;
///
/// assert_eq!(parse_duration("1000"), Ok(Duration::from_secs(1)));
/// ```
pub fn parse_duration(raw: &str) -> Result<Duration, ParseIntError> {
    Ok(Duration::from_millis(raw.parse()?))
}

/// Compare two elements wrapped into an option using provided comparison function. If at least one
/// of the elements is `None`, this will return false.
///
/// # Example
/// ```
/// use deduplicator::utils::*;
///
/// let same_sign = |x: &i64, y: &i64| (*x < 0) == (*y < 0);
///
/// assert!(field_compare(Some(3), Some(42), same_sign));
/// assert!(field_compare(Some(-1), Some(-10), same_sign));
///
/// assert!(!field_compare(Some(-1), Some(1), same_sign));
/// assert!(!field_compare(None, Some(0), same_sign));
/// assert!(!field_compare(None, None, same_sign));
/// ```
pub fn field_compare<T>(
    field1: impl Borrow<Option<T>>,
    field2: impl Borrow<Option<T>>,
    compare: impl Fn(&T, &T) -> bool,
) -> bool {
    match (field1.borrow().as_ref(), field2.borrow().as_ref()) {
        (Some(field1), Some(field2)) => compare(field1, field2),
        _ => false,
    }
}

/// Compare two elements wrapped into an option using provided comparison function. If one of the
/// elements is `None` and the other is not, this will return false.
///
/// # Example
/// ```
/// use deduplicator::utils::*;
///
/// let same_sign = |x: &i64, y: &i64| (*x < 0) == (*y < 0);
///
/// assert!(opt_field_compare(Some(3), Some(42), same_sign));
/// assert!(opt_field_compare(Some(-1), Some(-10), same_sign));
/// assert!(opt_field_compare(None, None, same_sign));
///
/// assert!(!opt_field_compare(Some(-1), Some(1), same_sign));
/// assert!(!opt_field_compare(None, Some(0), same_sign));
/// ```
pub fn opt_field_compare<T>(
    field1: impl Borrow<Option<T>>,
    field2: impl Borrow<Option<T>>,
    compare: impl Fn(&T, &T) -> bool,
) -> bool {
    match (field1.borrow().as_ref(), field2.borrow().as_ref()) {
        (None, None) => true,
        (Some(field1), Some(field2)) => compare(field1, field2),
        _ => false,
    }
}

/// Given an address, return its array reprensation used by libpostal.
///
/// # Example
/// ```
/// use deduplicator::utils::*;
/// use std::ffi::CString;
/// use tools::Address;
///
/// let address = Address {
///     number: Some("54".to_string()),
///     street: Some("rue des Koubis".to_string()),
///     city: Some("Paris".to_string()),
///     ..Address::default()
/// };
///
/// assert_eq!(postal_repr(&Address::default()), vec![]);
/// assert!(postal_repr(&address).contains(
///     &rpostal::Address {
///         label: CString::new("city".as_bytes()).unwrap(),
///         value: CString::new("Paris".as_bytes()).unwrap(),
///     }
/// ));
/// ```
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

/// Check if an SQLite error is a constraint violation.
///
/// # Example
/// ```
/// use deduplicator::utils::*;
/// use rusqlite::Connection;
///
/// assert!(!is_constraint_violation_error(&Connection::open("file/not/exists.sql").unwrap_err()));
/// ```
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

/// Load addresses from an SQLite file, into a deduplicator.
pub fn load_from_sqlite<F, R>(
    deduplication: &mut Deduplicator,
    path: PathBuf,
    filter: F,
    ranking: R,
    refresh_delay: Duration,
) -> rusqlite::Result<()>
where
    F: Fn(&Address) -> bool + Clone + Send + 'static,
    R: Fn(&Address) -> f64 + Clone + Send + 'static,
{
    let input_conn = Connection::open(&path)?;

    // Query list of addresses
    let mut stmt = input_conn.prepare("SELECT * FROM addresses;")?;
    let addresses = stmt
        .query_map(NO_PARAMS, |row| row.try_into())?
        .progress()
        .with_refresh_delay(refresh_delay)
        .with_prefix(format!("{:<45}", format!("{:?}", path)))
        .with_output_stream(prog_rs::OutputStream::StdErr)
        .filter_map(|addr| {
            addr.map_err(|e| teprintln!("Failed to read address from DB: {}", e))
                .ok()
        });

    // Insert addresses
    let mut inserter = deduplication.get_db_inserter(filter, ranking)?;

    for address in addresses {
        inserter.insert(address);
    }

    Ok(())
}
