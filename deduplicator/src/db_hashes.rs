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
        conn.pragma_update(None, "synchronous", &"OFF")?;
        conn.pragma_update(None, "journal_mode", &"OFF")?;

        conn.execute_batch(&format!(
            "
            CREATE TABLE IF NOT EXISTS {addresses} (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                lat         REAL NOT NULL,
                lon         REAL NOT NULL,
                number      TEXT,
                street      TEXT NOT NULL,
                unit        TEXT,
                city        TEXT,
                district    TEXT,
                region      TEXT,
                postcode    TEXT,
                rank        REAL
            );

            CREATE TABLE IF NOT EXISTS {hashes} (
                address     INTEGER NOT NULL,
                hash        INTEGER NOT NULL,
                PRIMARY KEY (address, hash)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS {to_delete} (
                address_id  INTEGER PRIMARY KEY
            );",
            addresses = TABLE_ADDRESSES,
            hashes = TABLE_HASHES,
            to_delete = TABLE_TO_DELETE
        ))?;

        Ok(Self { db_path })
    }

    pub fn get_conn(&self) -> rusqlite::Result<Connection> {
        Connection::open(&self.db_path)
    }

    pub fn create_hashes_index(&self) -> rusqlite::Result<()> {
        self.get_conn()?.execute_batch(&format!(
            "CREATE INDEX IF NOT EXISTS {hashes}_index_ ON {hashes} (hash);",
            hashes = TABLE_HASHES
        ))
    }

    fn count_table_entries(&self, table: &str) -> rusqlite::Result<isize> {
        self.get_conn()?.query_row(
            &format!("SELECT COUNT(*) FROM {};", table),
            NO_PARAMS,
            |row: &rusqlite::Row| row.get(0),
        )
    }

    pub fn count_addresses(&self) -> rusqlite::Result<isize> {
        self.count_table_entries(TABLE_ADDRESSES)
    }

    pub fn count_hashes(&self) -> rusqlite::Result<isize> {
        self.count_table_entries(TABLE_HASHES)
    }

    pub fn count_to_delete(&self) -> rusqlite::Result<isize> {
        self.count_table_entries(TABLE_TO_DELETE)
    }

    pub fn get_inserter<'c, 't>(
        tran: &'t mut Transaction<'c>,
    ) -> rusqlite::Result<Inserter<'c, 't>> {
        Inserter::new(tran)
    }

    pub fn get_sorted_hashes<'c>(conn: &'c Connection) -> rusqlite::Result<SortedHashesIter<'c>> {
        SortedHashesIter::prepare(conn)
    }

    pub fn apply_addresses_to_delete(&self) -> rusqlite::Result<usize> {
        self.get_conn()?.execute(
            &format!(
                "DELETE FROM {} WHERE id IN (SELECT address_id FROM {});",
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

    pub fn insert_hash(&mut self, address_id: i64, address_hash: i64) -> rusqlite::Result<()> {
        self.stmt_insert_hash.execute(&[address_id, address_hash])?;
        Ok(())
    }

    pub fn insert_to_delete(&mut self, address_id: i64) -> rusqlite::Result<()> {
        self.stmt_insert_to_delete
            .execute(std::iter::once(address_id))?;
        Ok(())
    }
}

//   ____      _ _ _     _                   ___ _
//  / ___|___ | | (_)___(_) ___  _ __  ___  |_ _| |_ ___ _ __
// | |   / _ \| | | / __| |/ _ \| '_ \/ __|  | || __/ _ \ '__|
// | |__| (_) | | | \__ \ | (_) | | | \__ \  | || ||  __/ |
//  \____\___/|_|_|_|___/_|\___/|_| |_|___/ |___|\__\___|_|
//

#[derive(Debug)]
pub struct HashIterItem {
    pub address: Address,
    pub hash: i64,
    pub id: i64,
    pub rank: f64,
}

pub struct SortedHashesIter<'c>(Statement<'c>);

impl<'c> SortedHashesIter<'c> {
    pub fn prepare(conn: &'c Connection) -> rusqlite::Result<Self> {
        Ok(SortedHashesIter(conn.prepare(&format!(
            "
                SELECT DISTINCT
                    addr.id         AS id,
                    addr.lat        AS lat,
                    addr.lon        AS lon,
                    addr.number     AS number,
                    addr.street     AS street,
                    addr.unit       AS unit,
                    addr.city       AS city,
                    addr.district   AS district,
                    addr.region     AS region,
                    addr.postcode   AS postcode,
                    addr.rank       AS rank,
                    hash.hash       AS hash
                FROM  {hashes}   AS hash
                JOIN {addresses} AS addr
                    ON hash.address = addr.id
                ORDER BY hash.hash;
            ",
            addresses = TABLE_ADDRESSES,
            hashes = TABLE_HASHES
        ))?))
    }

    pub fn iter<'s>(
        &'s mut self,
    ) -> rusqlite::Result<impl Iterator<Item = rusqlite::Result<HashIterItem>> + 's> {
        let Self(stmt) = self;

        Ok(stmt.query_map(NO_PARAMS, |row| {
            Ok(HashIterItem {
                address: Address::from_sqlite_row(&row)?,
                hash: row.get("hash")?,
                id: row.get("id")?,
                rank: row.get("rank")?,
            })
        })?)
    }
}
