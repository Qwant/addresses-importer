use rusqlite::{Connection, DropBehavior, Row, ToSql, NO_PARAMS};
use smartstring::alias::String;
use std::fs;

/// Returns a structure that displays as the current time under the form "HH:MM:SS".
pub fn get_time() -> impl std::fmt::Display {
    struct FormatTime(time::OffsetDateTime);

    impl std::fmt::Display for FormatTime {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "{:02}:{:02}:{:02}",
                self.0.hour(),
                self.0.minute(),
                self.0.second()
            )
        }
    }

    FormatTime(time::OffsetDateTime::now_utc())
}

/// Prints the message on stdout prepended by the current time.
///
/// Example:
///
/// ```
/// use tools::tprint;
///
/// tprint!("Something to print");
/// tprint!("Printing even more: {}", 32);
/// ```
#[macro_export]
macro_rules! tprint {
    ($format_str: literal $($arg:tt)*) => {{
        print!(concat!("[{}] ", $format_str), $crate::get_time() $($arg)*);
    }}
}

/// Prints the message on stderr prepended by the current time.
///
/// Example:
///
/// ```
/// use tools::teprint;
///
/// teprint!("Something to print");
/// teprint!("Printing even more: {}", 32);
/// ```
#[macro_export]
macro_rules! teprint {
    ($format_str: literal $($arg:tt)*) => {{
        eprint!(concat!("[{}] ", $format_str), $crate::get_time() $($arg)*);
    }}
}

#[macro_export]
macro_rules! tprintln {
    ($format_str: literal $($arg:tt)*) => {{
        println!(concat!("[{}] ", $format_str), $crate::get_time() $($arg)*);
    }}
}

#[macro_export]
macro_rules! teprintln {
    ($format_str: literal $($arg:tt)*) => {{
        eprintln!(concat!("[{}] ", $format_str), $crate::get_time() $($arg)*);
    }}
}

/// A type representing an address. Only the `lat` and `lon` fields aren't optional because all the
/// others might not be provided depending where we're getting the address from.
#[derive(Clone, Debug, Default, PartialOrd, PartialEq)]
pub struct Address {
    pub lat: f64,
    pub lon: f64,
    pub number: Option<String>,
    pub street: Option<String>,
    pub unit: Option<String>,
    pub city: Option<String>,
    pub district: Option<String>,
    pub region: Option<String>,
    pub postcode: Option<String>,
}

impl Address {
    pub const NB_FIELDS: usize = 9;

    /// Returns the number of not empty fields.
    ///
    /// Example:
    ///
    /// ```
    /// use tools::Address;
    ///
    /// let addr = Address {
    ///     lat: 0.,
    ///     lon: 0.,
    ///     number: Some("12".into()),
    ///     street: None,
    ///     unit: None,
    ///     city: None,
    ///     district: None,
    ///     region: None,
    ///     postcode: None,
    /// };
    /// assert_eq!(addr.count_non_empty_fields(), 3);
    /// ```
    pub fn count_non_empty_fields(&self) -> usize {
        2 // lon & lat
            + self.number.is_some() as usize
            + self.street.is_some() as usize
            + self.unit.is_some() as usize
            + self.city.is_some() as usize
            + self.district.is_some() as usize
            + self.region.is_some() as usize
            + self.postcode.is_some() as usize
    }
}

impl<'r> TryFrom<&Row<'r>> for Address {
    type Error = rusqlite::Error;

    fn try_from(row: &Row<'r>) -> Result<Self, Self::Error> {
        let get_string = |col| {
            Ok::<_, Self::Error>(
                row.get::<_, Option<std::string::String>>(col)?
                    .map(Into::into),
            )
        };

        Ok(Address {
            lat: row.get("lat")?,
            lon: row.get("lon")?,
            number: get_string("number")?,
            street: get_string("street")?,
            unit: get_string("unit")?,
            city: get_string("city")?,
            district: get_string("district")?,
            region: get_string("region")?,
            postcode: get_string("postcode")?,
        })
    }
}

/// Type holding a SQLite DB connection and handling interactions with it.
///
/// Note: When dropped, a flush is performed.
pub struct DB {
    conn: Connection,
    buffer: Vec<Address>,
    db_buffer_size: usize,
}

impl DB {
    /// Creates a new instance of `BD`.
    ///
    /// Arguments:
    ///
    /// * `db_files` is where the content will be stored.
    /// * `db_buffer_size` is how much can be stored in RAM.
    /// * if `remove_db_data` is set to `true`, any existing content will be removed.
    ///
    /// Example:
    ///
    /// ```no_run
    /// use tools::DB;
    ///
    /// let db = match DB::new("addresses.db", 10000, true) {
    ///     Ok(db) => db,
    ///     Err(e) => panic!("failed to create DB: {}", e),
    /// };
    /// ```
    pub fn new(db_file: &str, db_buffer_size: usize, remove_db_data: bool) -> Result<Self, String> {
        if remove_db_data {
            let _ = fs::remove_file(db_file); // we ignore any potential error
        }
        let conn = Connection::open(db_file)
            .map_err(|e| format!("failed to open SQLITE connection: {}", e))?;

        if remove_db_data {
            conn.execute("DROP TABLE IF EXISTS addresses", NO_PARAMS)
                .expect("failed to drop addresses");
            conn.execute("DROP TABLE IF EXISTS addresses_errors", NO_PARAMS)
                .expect("failed to drop errors");
        }
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS addresses(
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                number TEXT NOT NULL,
                street TEXT NOT NULL,
                unit TEXT,
                city TEXT,
                district TEXT,
                region TEXT,
                postcode TEXT,
                PRIMARY KEY (lat, lon, number, street, city)
            )"#,
            NO_PARAMS,
        )
        .map_err(|e| format!("failed to create table: {}", e))?;
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS addresses_errors(
                lat REAL,
                lon REAL,
                number TEXT,
                street TEXT,
                unit TEXT,
                city TEXT,
                district TEXT,
                region TEXT,
                postcode TEXT,
                kind TEXT
            )"#,
            NO_PARAMS,
        )
        .map_err(|e| format!("failed to create error table: {}", e))?;
        Ok(DB {
            conn,
            buffer: Vec::with_capacity(db_buffer_size),
            db_buffer_size,
        })
    }

    /// Flushes all on-hold data.
    ///
    /// Example:
    ///
    /// ```no_run
    /// use tools::{Address, CompatibleDB, DB};
    ///
    /// let mut db = DB::new("addresses.db", 10000, true).expect("failed to create DB");
    /// db.insert(Address {
    ///     lat: 0.,
    ///     lon: 0.,
    ///     number: Some("12".into()),
    ///     street: Some("rue des champignons".into()),
    ///     unit: None,
    ///     city: None,
    ///     district: None,
    ///     region: None,
    ///     postcode: None,
    /// });
    /// db.flush();
    /// ```
    pub fn flush(&mut self) {
        let mut tx = self.conn.transaction().expect("failed to open transaction");
        tx.set_drop_behavior(DropBehavior::Ignore);

        let mut errors = {
            let mut stmt = tx
                .prepare(
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
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                )
                .expect("failed to prepare statement");

            self.buffer
                .drain(..)
                .filter_map(|obj| {
                    if let Err(e) = stmt.execute(&[
                        &obj.lat as &dyn ToSql,
                        &obj.lon,
                        &obj.number.as_ref().map(|s| s.as_str()),
                        &obj.street.as_ref().map(|s| s.as_str()),
                        &obj.unit.as_ref().map(|s| s.as_str()),
                        &obj.city.as_ref().map(|s| s.as_str()),
                        &obj.district.as_ref().map(|s| s.as_str()),
                        &obj.region.as_ref().map(|s| s.as_str()),
                        &obj.postcode.as_ref().map(|s| s.as_str()),
                    ]) {
                        Some((obj, e.to_string()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        if !errors.is_empty() {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO addresses_errors(
                    lat,
                    lon,
                    number,
                    street,
                    unit,
                    city,
                    district,
                    region,
                    postcode,
                    kind
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                )
                .expect("failed to prepare error statement");

            for (obj, err) in errors.drain(..) {
                stmt.execute(&[
                    &obj.lat as &dyn ToSql,
                    &obj.lon,
                    &obj.number.as_ref().map(|s| s.as_str()),
                    &obj.street.as_ref().map(|s| s.as_str()),
                    &obj.unit.as_ref().map(|s| s.as_str()),
                    &obj.city.as_ref().map(|s| s.as_str()),
                    &obj.district.as_ref().map(|s| s.as_str()),
                    &obj.region.as_ref().map(|s| s.as_str()),
                    &obj.postcode.as_ref().map(|s| s.as_str()),
                    &err,
                ])
                .expect("failed to insert into errors");
            }
        }

        tx.commit().expect("commit failed");
    }
}

/// A trait used by importers. If you want to use another type than `DB`, you'll have to implement
/// this trait on it.
pub trait CompatibleDB {
    /// Inserts a new address.
    ///
    /// Example:
    ///
    /// ```no_run
    /// use tools::{Address, CompatibleDB, DB};
    ///
    /// let mut db = DB::new("addresses.db", 10000, true).expect("failed to create DB");
    /// db.insert(Address {
    ///     lat: 0.,
    ///     lon: 0.,
    ///     number: Some("12".into()),
    ///     street: Some("rue des champignons".into()),
    ///     unit: None,
    ///     city: None,
    ///     district: None,
    ///     region: None,
    ///     postcode: None,
    /// });
    /// ```
    fn insert(&mut self, addr: Address);
    /// Counts the number of different inserted cities.
    ///
    /// Example:
    ///
    /// ```no_run
    /// use tools::{Address, CompatibleDB, DB};
    ///
    /// let mut db = DB::new("addresses.db", 10000, true).expect("failed to create DB");
    /// assert_eq!(db.get_nb_cities(), 0);
    /// db.insert(Address {
    ///     lat: 0.,
    ///     lon: 0.,
    ///     number: Some("12".into()),
    ///     street: Some("rue des champignons".into()),
    ///     unit: None,
    ///     city: Some("Paris".into()),
    ///     district: None,
    ///     region: None,
    ///     postcode: None,
    /// });
    /// assert_eq!(db.get_nb_cities(), 1);
    /// ```
    fn get_nb_cities(&mut self) -> i64;
    /// Counts the number of inserted addresses.
    ///
    /// Example:
    ///
    /// ```no_run
    /// use tools::{Address, CompatibleDB, DB};
    ///
    /// let mut db = DB::new("addresses.db", 10000, true).expect("failed to create DB");
    /// assert_eq!(db.get_nb_addresses(), 0);
    /// db.insert(Address {
    ///     lat: 0.,
    ///     lon: 0.,
    ///     number: Some("12".into()),
    ///     street: Some("rue des champignons".into()),
    ///     unit: None,
    ///     city: None,
    ///     district: None,
    ///     region: None,
    ///     postcode: None,
    /// });
    /// assert_eq!(db.get_nb_addresses(), 1);
    /// ```
    fn get_nb_addresses(&mut self) -> i64;
    /// Returnss the number of errors that occurred. An error occurs generally when an address is
    /// considered as a duplicate or is missing one of the mandatory field (such as the street name
    /// for example).
    ///
    /// Example:
    ///
    /// ```no_run
    /// use tools::{Address, CompatibleDB, DB};
    ///
    /// let mut db = DB::new("addresses.db", 10000, true).expect("failed to create DB");
    /// assert_eq!(db.get_nb_addresses(), 0);
    /// db.insert(Address {
    ///     lat: 0.,
    ///     lon: 0.,
    ///     number: Some("12".into()),
    ///     street: None,
    ///     unit: None,
    ///     city: None,
    ///     district: None,
    ///     region: None,
    ///     postcode: None,
    /// });
    /// assert_eq!(db.get_nb_errors(), 1);
    /// ```
    fn get_nb_errors(&mut self) -> i64;
    /// Returns the number of errors grouped by kind. A kind is determined by the DB returned error
    /// generally.
    ///
    /// Example:
    ///
    /// ```no_run
    /// use tools::{Address, CompatibleDB, DB};
    ///
    /// let mut db = DB::new("addresses.db", 10000, true).expect("failed to create DB");
    /// assert_eq!(db.get_nb_addresses(), 0);
    /// db.insert(Address {
    ///     lat: 0.,
    ///     lon: 0.,
    ///     number: Some("12".into()),
    ///     street: None,
    ///     unit: None,
    ///     city: None,
    ///     district: None,
    ///     region: None,
    ///     postcode: None,
    /// });
    /// assert_eq!(db.get_nb_by_errors_kind(), vec![("Missing mandataory field".into(), 1)]);
    /// ```
    fn get_nb_by_errors_kind(&mut self) -> Vec<(std::string::String, i64)>;
    /// Returns a list of addresses matching the given housenumber and street name.
    ///
    /// Example:
    ///
    /// ```no_run
    /// use tools::{Address, CompatibleDB, DB};
    ///
    /// let mut db = DB::new("addresses.db", 10000, true).expect("failed to create DB");
    /// assert_eq!(db.get_nb_addresses(), 0);
    /// db.insert(Address {
    ///     lat: 0.,
    ///     lon: 0.,
    ///     number: Some("12".into()),
    ///     street: Some("rue des champignons".into()),
    ///     unit: None,
    ///     city: None,
    ///     district: None,
    ///     region: None,
    ///     postcode: None,
    /// });
    /// assert_eq!(db.get_address(12, "rue des champignons"),
    ///            vec![Address {
    ///                 lat: 0.,
    ///                 lon: 0.,
    ///                 number: Some("12".into()),
    ///                 street: Some("rue des champignons".into()),
    ///                 unit: None,
    ///                 city: None,
    ///                 district: None,
    ///                 region: None,
    ///                 postcode: None,
    ///             }]);
    /// ```
    fn get_address(&mut self, housenumber: i32, street: &str) -> Vec<Address>;
}

impl CompatibleDB for DB {
    fn insert(&mut self, addr: Address) {
        if addr.street.is_none() || addr.number.is_none() {
            return;
        }
        self.buffer.push(addr);
        if self.buffer.len() >= self.db_buffer_size {
            self.flush();
        }
    }

    fn get_nb_cities(&mut self) -> i64 {
        self.flush();
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(DISTINCT city) FROM addresses;")
            .expect("failed to prepare");
        let mut iter = stmt
            .query_map(NO_PARAMS, |row| row.get(0))
            .expect("query_map failed");
        iter.next().expect("no count???").expect("failed")
    }

    fn get_nb_addresses(&mut self) -> i64 {
        self.flush();
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM addresses")
            .expect("failed to prepare");
        let mut iter = stmt
            .query_map(NO_PARAMS, |row| row.get(0))
            .expect("query_map failed");
        let x: i64 = iter.next().expect("no count???").expect("failed");
        x + self.buffer.len() as i64
    }

    fn get_nb_errors(&mut self) -> i64 {
        self.flush();
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM addresses_errors")
            .expect("failed to prepare");
        let mut iter = stmt
            .query_map(NO_PARAMS, |row| row.get(0))
            .expect("query_map failed");
        iter.next().expect("no count???").expect("failed")
    }

    fn get_nb_by_errors_kind(&mut self) -> Vec<(std::string::String, i64)> {
        self.flush();
        let mut stmt = self
            .conn
            .prepare("SELECT kind, COUNT(*) FROM addresses_errors GROUP BY kind")
            .expect("failed to prepare");
        stmt.query_map(NO_PARAMS, |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("query_map failed")
            .map(|x| x.expect("failed"))
            .collect()
    }

    fn get_address(&mut self, housenumber: i32, street: &str) -> Vec<Address> {
        self.flush();
        let mut stmt = self.conn
            .prepare("SELECT lat, lon, number, street, unit, city, district, region, postcode FROM addresses WHERE number=?1 AND street=?2")
            .expect("failed to prepare statement");
        stmt.query_map(&[&housenumber as &dyn ToSql, &street], |row| row.try_into())
            .expect("failed to insert into errors")
            .map(|x| x.expect("failed parsing address"))
            .collect()
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        self.flush();
    }
}
