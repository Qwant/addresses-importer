use std::path::PathBuf;

use rusqlite::{Connection, Statement, ToSql, Transaction, NO_PARAMS};

use crate::address::Address;

const TABLE_ADDRESSES: &str = "addresses";
const TABLE_HASHES: &str = "_addresses_hashes";
const TABLE_TO_DELETE: &str = "_to_delete";

//  ____  _     _   _           _
// |  _ \| |__ | | | | __ _ ___| |__   ___  ___
// | | | | '_ \| |_| |/ _` / __| '_ \ / _ \/ __|
// | |_| | |_) |  _  | (_| \__ \ | | |  __/\__ \
// |____/|_.__/|_| |_|\__,_|___/_| |_|\___||___/
//

pub struct DbHashes {
    db_path: PathBuf,
}

impl DbHashes {
    pub fn new(db_path: PathBuf) -> rusqlite::Result<Self> {
        let conn = Connection::open(&db_path)?;

        conn.pragma_update(None, "page_size", &4096)?;
        conn.pragma_update(None, "cache_size", &10_000)?;
        conn.pragma_update(None, "synchronous", &"NORMAL")?;
        conn.pragma_update(None, "journal_mode", &"OFF")?;

        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                lat         REAL NOT NULL,
                lon         REAL NOT NULL,
                number      TEXT,
                street      TEXT NOT NULL,
                unit        TEXT,
                city        TEXT,
                district    TEXT,
                region      TEXT,
                postcode    TEXT,
                rank        REAL,
                PRIMARY KEY (lat, lon, number, street, city)
            );

            CREATE TABLE IF NOT EXISTS {} (
                address     INTEGER NOT NULL,
                hash        INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS {} (
                address_id  INTEGER NOT NULL
            );",
            TABLE_ADDRESSES, TABLE_HASHES, TABLE_TO_DELETE
        ))?;

        Ok(Self { db_path })
    }

    pub fn get_conn(&self) -> rusqlite::Result<Connection> {
        Connection::open(&self.db_path)
    }

    pub fn count_addresses(&self) -> rusqlite::Result<isize> {
        self.get_conn()?.query_row(
            &format!("SELECT COUNT(*) FROM {};", TABLE_ADDRESSES),
            NO_PARAMS,
            |row: &rusqlite::Row| row.get(0),
        )
    }

    pub fn count_to_delete(&self) -> rusqlite::Result<isize> {
        self.get_conn()?.query_row(
            &format!("SELECT COUNT(*) FROM {};", TABLE_TO_DELETE),
            NO_PARAMS,
            |row: &rusqlite::Row| row.get(0),
        )
    }

    pub fn get_inserter<'c, 't>(
        tran: &'t mut Transaction<'c>,
    ) -> rusqlite::Result<Inserter<'c, 't>> {
        Inserter::new(tran)
    }

    pub fn feasible_duplicates<'c>(
        conn: &'c Connection,
    ) -> rusqlite::Result<CollisionsIterable<'c>> {
        CollisionsIterable::prepare(conn)
    }

    pub fn apply_addresses_to_delete(&self) -> rusqlite::Result<usize> {
        self.get_conn()?.execute(
            &format!(
                "DELETE FROM {} WHERE rowid IN (SELECT address_id FROM {});",
                TABLE_ADDRESSES, TABLE_TO_DELETE
            ),
            NO_PARAMS,
        )
    }

    pub fn cleanup_database(&self) -> rusqlite::Result<()> {
        let conn = self.get_conn()?;

        for db in [TABLE_HASHES, TABLE_TO_DELETE].into_iter() {
            conn.execute_batch(&format!("DROP TABLE {};", db))?;
        }

        Ok(())
    }
}

//  ___                     _
// |_ _|_ __  ___  ___ _ __| |_ ___ _ __
//  | || '_ \/ __|/ _ \ '__| __/ _ \ '__|
//  | || | | \__ \  __/ |  | ||  __/ |
// |___|_| |_|___/\___|_|   \__\___|_|
//

pub struct Inserter<'c, 't> {
    tran: &'t Transaction<'c>,
    stmt_insert_address: Statement<'t>,
    stmt_insert_hash: Statement<'t>,
    stmt_insert_to_delete: Statement<'t>,
}

impl<'c, 't> Inserter<'c, 't> {
    pub fn new(tran: &'t mut Transaction<'c>) -> rusqlite::Result<Self> {
        let stmt_insert_address = tran.prepare(&format!(
            "INSERT INTO {} (
                lat,
                lon,
                number,
                street,
                unit,
                city,
                district,
                region,
                postcode,
                rank
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10);",
            TABLE_ADDRESSES
        ))?;

        let stmt_insert_hash = tran.prepare(&format!(
            "INSERT INTO {} (address, hash) VALUES (?1, ?2);",
            TABLE_HASHES
        ))?;

        let stmt_insert_to_delete = tran.prepare(&format!(
            "INSERT INTO {} (address_id) VALUES (?1);",
            TABLE_TO_DELETE
        ))?;

        Ok(Self {
            tran,
            stmt_insert_address,
            stmt_insert_hash,
            stmt_insert_to_delete,
        })
    }

    pub fn insert_address(&mut self, address: &Address, rank: f64) -> rusqlite::Result<i64> {
        self.stmt_insert_address.execute(&[
            &address.lat as &dyn ToSql,
            &address.lon,
            &address.number,
            &address.street,
            &address.unit,
            &address.city,
            &address.district,
            &address.region,
            &address.postcode,
            &rank,
        ])?;
        Ok(self.tran.last_insert_rowid())
    }

    pub fn insert_hash(&mut self, address_id: i64, address_hash: i64) -> rusqlite::Result<i64> {
        self.stmt_insert_hash.execute(&[address_id, address_hash])?;
        Ok(self.tran.last_insert_rowid())
    }

    pub fn insert_to_delete(&mut self, address_id: i64) -> rusqlite::Result<i64> {
        self.stmt_insert_to_delete
            .execute(std::iter::once(address_id))?;
        Ok(self.tran.last_insert_rowid())
    }
}

//   ____      _ _ _     _                   ___ _
//  / ___|___ | | (_)___(_) ___  _ __  ___  |_ _| |_ ___ _ __
// | |   / _ \| | | / __| |/ _ \| '_ \/ __|  | || __/ _ \ '__|
// | |__| (_) | | | \__ \ | (_) | | | \__ \  | || ||  __/ |
//  \____\___/|_|_|_|___/_|\___/|_| |_|___/ |___|\__\___|_|
//

pub struct CollisionsIterable<'c>(Statement<'c>);

impl<'c> CollisionsIterable<'c> {
    pub fn prepare(conn: &'c Connection) -> rusqlite::Result<Self> {
        Ok(CollisionsIterable(conn.prepare(&format!(
            "
                SELECT DISTINCT
                    addr_1.rowid        AS addr_1_id,
                    addr_1.lat          AS addr_1_lat,
                    addr_1.lon          AS addr_1_lon,
                    addr_1.number       AS addr_1_number,
                    addr_1.street       AS addr_1_street,
                    addr_1.unit         AS addr_1_unit,
                    addr_1.city         AS addr_1_city,
                    addr_1.district     AS addr_1_district,
                    addr_1.region       AS addr_1_region,
                    addr_1.postcode     AS addr_1_postcode,
                    addr_1.rank         AS addr_1_rank,
                    addr_2.rowid        AS addr_2_id,
                    addr_2.lat          AS addr_2_lat,
                    addr_2.lon          AS addr_2_lon,
                    addr_2.number       AS addr_2_number,
                    addr_2.street       AS addr_2_street,
                    addr_2.unit         AS addr_2_unit,
                    addr_2.city         AS addr_2_city,
                    addr_2.district     AS addr_2_district,
                    addr_2.region       AS addr_2_region,
                    addr_2.postcode     AS addr_2_postcode,
                    addr_2.rank         AS addr_2_rank
                FROM {addresses} AS addr_1
                JOIN {addresses} AS addr_2
                JOIN {hashes} AS hash_1 ON addr_1.rowid = hash_1.address
                JOIN {hashes} AS hash_2 ON addr_2.rowid = hash_2.address
                WHERE addr_1.rowid < addr_2.rowid AND hash_1.hash = hash_2.hash;
            ",
            addresses = TABLE_ADDRESSES,
            hashes = TABLE_HASHES
        ))?))
    }

    pub fn iter<'s>(
        &'s mut self,
    ) -> rusqlite::Result<
        impl Iterator<Item = rusqlite::Result<((i64, Address, f64), (i64, Address, f64))>> + 's,
    > {
        let Self(stmt) = self;

        Ok(stmt.query_map(NO_PARAMS, |row| {
            Ok((
                (
                    row.get("addr_1_id")?,
                    address_from_sqlite_row_with_prefix!("addr_1_", row)?,
                    row.get("addr_1_rank")?,
                ),
                (
                    row.get("addr_2_id")?,
                    address_from_sqlite_row_with_prefix!("addr_2_", row)?,
                    row.get("addr_2_rank")?,
                ),
            ))
        })?)
    }
}