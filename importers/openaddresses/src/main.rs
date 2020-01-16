use std::collections::HashMap;
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::path::Path;
use std::str::FromStr;

use rusqlite::{Connection, DropBehavior, ToSql, NO_PARAMS};
use csv::{Reader, StringRecord};

const BUFFER_SIZE: usize = 1000;

macro_rules! get_with_headers {
    ($headers:expr, $key:expr, $records:expr) => {
        if let Some(index) = $headers.0.get($key) {
            $records.get(*index)
        } else {
            None
        }
    }
}

struct Address<'a, 'b, 'c, 'd, 'e, 'f> {
    lat: Option<f64>,
    lon: Option<f64>,
    number: Option<&'a str>,
    street: Option<&'b str>,
    unit: Option<&'c str>,
    city: Option<&'d str>,
    district: Option<&'d str>,
    region: Option<&'e str>,
    postcode: Option<&'f str>,
}

struct DB {
    conn: Connection,
}

impl DB {
    fn new(db_file: &str) -> Result<DB, String> {
        // let _ = fs::remove_file(db_file); // we ignore any potential error
        let conn = Connection::open(db_file)
            .map_err(|e| format!("failed to open SQLITE connection: {}", e))?;

        conn.execute(
            "DROP TABLE IF EXISTS addresses",
            NO_PARAMS,
        ).expect("failed to drop addresses");
        conn.execute(
            "DROP TABLE IF EXISTS addresses_errors",
            NO_PARAMS,
        ).expect("failed to drop errors");
        conn.execute(
            r#"CREATE TABLE addresses(
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
            )"#,
            NO_PARAMS,
        ).map_err(|e| format!("failed to create table: {}", e))?;
        conn.execute(
            r#"CREATE TABLE addresses_errors(
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
        ).map_err(|e| format!("failed to create error table: {}", e))?;
        Ok(DB { conn })
    }

    fn insert(&mut self, headers: &Headers, addrs: &[StringRecord]) {
        let mut tx = self.conn.transaction().expect("failed to open transaction");
        tx.set_drop_behavior(DropBehavior::Ignore);

        let mut errors = {
            let mut stmt = tx.prepare(
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
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
            ).expect("failed to prepare statement");

            addrs.iter().filter_map(|x| {
                let obj = Address {
                    lat: get_with_headers!(headers, "lat", x).and_then(|x| f64::from_str(x).ok()),
                    lon: get_with_headers!(headers, "lon", x).and_then(|x| f64::from_str(x).ok()),
                    number: get_with_headers!(headers, "number", x),
                    street: get_with_headers!(headers, "street", x),
                    unit: get_with_headers!(headers, "unit", x),
                    city: get_with_headers!(headers, "city", x),
                    district: get_with_headers!(headers, "district", x),
                    region: get_with_headers!(headers, "region", x),
                    postcode: get_with_headers!(headers, "postcode", x),
                };
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
            }).collect::<Vec<_>>()
        };
        if !errors.is_empty() {
            let mut stmt = tx.prepare(
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
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
            ).expect("failed to prepare error statement");

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
                ]).expect("failed to insert into errors");
            }
        }

        tx.commit().expect("commit failed");
    }

    fn get_nb_cities(&self) -> i64 {
        let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM addresses GROUP BY city").expect("failed to prepare");
        let mut iter = stmt.query_map(NO_PARAMS, |row| {
            Ok(row.get(0)?)
        }).expect("query_map failed");
        iter.next().expect("no count???").expect("failed")
    }

    fn get_nb_addresses(&self) -> i64 {
        let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM addresses").expect("failed to prepare");
        let mut iter = stmt.query_map(NO_PARAMS, |row| {
            Ok(row.get(0)?)
        }).expect("query_map failed");
        iter.next().expect("no count???").expect("failed")
    }

    fn get_nb_errors(&self) -> i64 {
        let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM addresses_errors").expect("failed to prepare");
        let mut iter = stmt.query_map(NO_PARAMS, |row| {
            Ok(row.get(0)?)
        }).expect("query_map failed");
        iter.next().expect("no count???").expect("failed")
    }

    fn get_nb_by_errors_kind(&self) -> Vec<(String, i64)> {
        let mut stmt = self.conn.prepare("SELECT kind, COUNT(*) FROM addresses_errors GROUP BY kind").expect("failed to prepare");
        stmt.query_map(NO_PARAMS, |row| {
            Ok((row.get(0)?, row.get(1)?))
        }).expect("query_map failed")
        .map(|x| x.expect("failed"))
        .collect()
    }
}

struct Headers(HashMap<String, usize>);

impl Headers {
    fn new(headers: &StringRecord) -> Headers {
        let mut this = Headers(HashMap::with_capacity(11));

        for (pos, header) in headers.iter().enumerate() {
            this.0.insert(header.to_lowercase(), pos);
        }
        this
    }
}

fn read_csv<P: AsRef<Path>>(db: &mut DB, file_path: P) {
    let file = File::open(file_path).expect("cannot open file");
    let mut rdr = Reader::from_reader(file);
    let headers = Headers::new(&rdr.headers().expect("no headers found"));

    let mut records = rdr.into_records();
    let mut buffer = Vec::with_capacity(BUFFER_SIZE);
    while let Some(x) = records.next() {
        match x {
            Ok(x) => {
                buffer.push(x);
            }
            Err(e) => {
                eprintln!("invalid record found: {}", e);
                continue
            }
        }
        if buffer.len() >= BUFFER_SIZE {
            db.insert(&headers, &buffer);
            buffer.truncate(0);
        }
    }
    if !buffer.is_empty() {
        db.insert(&headers, &buffer);
    }
}

fn visit_dirs<P: AsRef<Path>>(path: P, db: &mut DB) {
    for entry in fs::read_dir(path).expect("folder not found") {
        if let Ok(entry) = entry {
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, db);
            } else if path.extension().unwrap_or_else(|| OsStr::new("")) == "csv" {
                read_csv(db, &path);
            }
        }
    }
}

fn main() {
    let args = env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        eprintln!("Expected openaddresses folder");
        return
    }

    let mut db = DB::new("addresses.db").expect("failed to create DB");
    visit_dirs(&args[1], &mut db);

    println!("Got {} addresses in {} cities (and {} errors)",
        db.get_nb_addresses(),
        db.get_nb_cities(),
        db.get_nb_errors(),
    );
    println!("Errors by categories:");
    let rows = db.get_nb_by_errors_kind();
    for (kind, nb) in rows {
        println!("  {} => {} occurences", kind, nb);
    }
}
