use rusqlite::{Connection, DropBehavior, Row, ToSql, NO_PARAMS};
use std::convert::{TryFrom, TryInto};
use std::fs;

pub fn get_time() -> String {
    let now = time::Time::now();
    format!("{:02}:{:02}:{:02}", now.hour(), now.minute(), now.second())
}

#[macro_export]
macro_rules! tprint {
    ($($arg:tt)*) => {{
        use tools::get_time;
        println!("[{}] {}", get_time(), format!($($arg)*));
    }}
}

#[macro_export]
macro_rules! teprint {
    ($($arg:tt)*) => {{
        use tools::get_time;
        eprintln!("[{}] {}", get_time(), format!($($arg)*));
    }}
}

#[derive(Clone, Debug, PartialOrd, PartialEq)]
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
    }
}

pub struct DB {
    conn: Connection,
    buffer: Vec<Address>,
    db_buffer_size: usize,
}

impl DB {
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
}

pub trait CompatibleDB {
    fn flush(&mut self);
    fn insert(&mut self, addr: Address);
    fn get_nb_cities(&mut self) -> i64;
    fn get_nb_addresses(&mut self) -> i64;
    fn get_nb_errors(&mut self) -> i64;
    fn get_nb_by_errors_kind(&mut self) -> Vec<(String, i64)>;
    fn get_address(&mut self, housenumber: i32, street: &str) -> Vec<Address>;
}

impl CompatibleDB for DB {
    fn flush(&mut self) {
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
                        &obj.number,
                        &obj.street,
                        &obj.unit,
                        &obj.city,
                        &obj.district,
                        &obj.region,
                        &obj.postcode,
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
                    &obj.number,
                    &obj.street,
                    &obj.unit,
                    &obj.city,
                    &obj.district,
                    &obj.region,
                    &obj.postcode,
                    &err,
                ])
                .expect("failed to insert into errors");
            }
        }

        tx.commit().expect("commit failed");
    }

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
            .query_map(NO_PARAMS, |row| Ok(row.get(0)?))
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
            .query_map(NO_PARAMS, |row| Ok(row.get(0)?))
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
            .query_map(NO_PARAMS, |row| Ok(row.get(0)?))
            .expect("query_map failed");
        iter.next().expect("no count???").expect("failed")
    }

    fn get_nb_by_errors_kind(&mut self) -> Vec<(String, i64)> {
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
