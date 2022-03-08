//! Definitions of the structure of a working database to save addresses and how to interact with
//! it. This database will be used to save hashes of all imported addresses and compute collisions
//! between them.

use std::{collections::HashSet, path::PathBuf};

use rusqlite::{Connection, Statement, ToSql, Transaction};
use tools::Address;

use crate::utils::partition;

/// Name of the table containing addresses.
const TABLE_ADDRESSES: &str = "addresses";

/// Name of the table containing hashes. For each address, multiple hashes may be stored.
const TABLE_HASHES: &str = "_addresses_hashes";

/// A database, this structure can be used to open connections or perform high-level operations.
pub struct DbHashes {
    db_path: PathBuf,
    to_delete: HashSet<i64>,
}

impl DbHashes {
    /// Instantiate a new database from a path to an SQLite file.
    ///
    /// If the file is not created yet or if the schema is not already set up, this will be done.
    ///
    /// # Example
    /// ```no_run
    /// use deduplicator::db_hashes::*;
    /// let db = DbHashes::new("sqlite.db".into(), None).unwrap();
    /// ```
    pub fn new(db_path: PathBuf, cache_size: Option<u32>) -> rusqlite::Result<Self> {
        let conn = Connection::open(&db_path)?;
        let cache_size = cache_size.unwrap_or(10_000);

        conn.pragma_update(None, "page_size", &4096)?;
        conn.pragma_update(None, "cache_size", &cache_size)?;
        conn.pragma_update(None, "synchronous", &"OFF")?;
        conn.pragma_update(None, "journal_mode", &"OFF")?;

        conn.execute_batch(&format!(
            "
                CREATE TABLE IF NOT EXISTS {TABLE_ADDRESSES} (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    lat         REAL NOT NULL,
                    lon         REAL NOT NULL,
                    number      TEXT NOT NULL,
                    street      TEXT NOT NULL,
                    unit        TEXT,
                    city        TEXT,
                    district    TEXT,
                    region      TEXT,
                    postcode    TEXT,
                    rank        REAL
                );

                CREATE TABLE IF NOT EXISTS {TABLE_HASHES} (
                    address     INTEGER NOT NULL,
                    hash        INTEGER NOT NULL,
                    PRIMARY KEY (address, hash)
                ) WITHOUT ROWID;
            "
        ))?;

        Ok(Self {
            db_path,
            to_delete: HashSet::new(),
        })
    }

    /// Open a connection to the database.
    ///
    /// # Example
    /// ```no_run
    /// use deduplicator::db_hashes::*;
    /// use rusqlite::NO_PARAMS;
    ///
    /// let db = DbHashes::new("sqlite.db".into(), None).unwrap();
    /// let conn = db.get_conn().unwrap();
    /// ```
    pub fn get_conn(&self) -> rusqlite::Result<Connection> {
        Connection::open(&self.db_path)
    }

    /// Index hashes by value, this will help computing collisions.
    ///
    /// Note that this operation will probably automatically be scheduled by the query planner if
    /// you omit to call this function, thus it is mainly intended to call this for monitoring
    /// purposes.
    pub fn create_hashes_index(&self) -> rusqlite::Result<()> {
        self.get_conn()?.execute_batch(&format!(
            "CREATE INDEX IF NOT EXISTS {TABLE_HASHES}_index ON {TABLE_HASHES} (hash);"
        ))
    }

    /// Return a list of addresses matching an input house number and street name.
    pub fn get_addresses_by_street(
        &self,
        housenumber: i32,
        street: &str,
    ) -> rusqlite::Result<Vec<Address>> {
        let conn = self.get_conn()?;
        let mut stmt = conn
            .prepare(&format!(
                "SELECT * FROM {TABLE_ADDRESSES} WHERE number=?1 AND street=?2;"
            ))
            .expect("failed to prepare statement");

        let mut addr_iter =
            stmt.query_map(&[&housenumber, &street as &dyn ToSql], |row| row.try_into())?;

        addr_iter.try_fold(Vec::new(), |mut acc, addr| {
            acc.push(addr?);
            Ok(acc)
        })
    }

    /// Returns the number of rows of a table.
    fn count_table_entries(&self, table: &str) -> rusqlite::Result<i64> {
        self.get_conn()?.query_row(
            &format!("SELECT COUNT(*) FROM {table};"),
            [],
            |row: &rusqlite::Row| row.get(0),
        )
    }

    /// Returns the number of addresses in the database.
    ///
    /// # Example
    /// ```no_run
    /// use deduplicator::db_hashes::*;
    ///
    /// let db = DbHashes::new("sqlite.db".into(), None).unwrap();
    /// assert_eq!(db.count_addresses(), Ok(0));
    /// ```
    pub fn count_addresses(&self) -> rusqlite::Result<i64> {
        self.count_table_entries(TABLE_ADDRESSES)
    }

    /// Returns the number of hashes in the database.
    ///
    /// # Example
    /// ```no_run
    /// use deduplicator::db_hashes::*;
    ///
    /// let db = DbHashes::new("sqlite.db".into(), None).unwrap();
    /// assert_eq!(db.count_hashes(), Ok(0));
    /// ```
    pub fn count_hashes(&self) -> rusqlite::Result<i64> {
        self.count_table_entries(TABLE_HASHES)
    }

    /// Returns the number of addresses intended to be deleted in the database.
    ///
    /// # Example
    /// ```no_run
    /// use deduplicator::db_hashes::*;
    ///
    /// let db = DbHashes::new("sqlite.db".into(), None).unwrap();
    /// assert_eq!(db.count_to_delete(), 0);
    /// ```
    pub fn count_to_delete(&self) -> usize {
        self.to_delete.len()
    }

    /// Returns the number of cities in the database.
    ///
    /// # Example
    /// ```no_run
    /// use deduplicator::db_hashes::*;
    ///
    /// let db = DbHashes::new("sqlite.db".into(), None).unwrap();
    /// assert_eq!(db.count_cities(), Ok(0));
    /// ```
    pub fn count_cities(&self) -> rusqlite::Result<i64> {
        self.get_conn()?.query_row(
            &format!("SELECT COUNT(DISTINCT city) FROM {TABLE_ADDRESSES};"),
            [],
            |row: &rusqlite::Row| row.get(0),
        )
    }

    /// Count the number of pairs (address, hash) that are in collision with another.
    ///
    /// # Example
    /// ```no_run
    /// use deduplicator::db_hashes::*;
    ///
    /// let db = DbHashes::new("sqlite.db".into(), None).unwrap();
    /// assert_eq!(db.count_collisions(), Ok(0));
    /// ```
    pub fn count_collisions(&self) -> rusqlite::Result<i64> {
        self.get_conn()?.query_row(
            &format!(
                "
                    SELECT SUM(count)
                    FROM (
                        SELECT COUNT(*) AS count
                        FROM {TABLE_HASHES}
                        GROUP BY hash
                        HAVING count > 1
                    );
                "
            ),
            [],
            |row: &rusqlite::Row| row.get(0),
        )
    }

    /// Get an inserter for the database. This will materialize as a transaction that can be used
    /// to efficiently insert data in the database.
    ///
    /// # Example
    /// ```no_run
    /// use deduplicator::db_hashes::*;
    ///
    /// use tools::Address;
    /// use rusqlite::DropBehavior;
    ///
    /// let db = DbHashes::new("sqlite.db".into(), None).unwrap();
    /// assert_eq!(db.count_addresses(), Ok(0));
    ///
    /// {
    ///     let addr = Address {
    ///         number: Some("24 bis".into()),
    ///         street: Some("rue des serpentins".into()),
    ///         ..Address::default()
    ///     };
    ///
    ///     let mut conn = db.get_conn().unwrap();
    ///     let mut tran = conn.transaction().unwrap();
    ///     tran.set_drop_behavior(DropBehavior::Commit);
    ///
    ///     let mut inserter = DbHashes::get_inserter(&mut tran).unwrap();
    ///     inserter.insert_address(&addr, 1.0).unwrap();
    /// }
    ///
    /// assert_eq!(db.count_addresses(), Ok(1));
    /// ```
    pub fn get_inserter<'c, 't>(
        tran: &'t mut Transaction<'c>,
    ) -> rusqlite::Result<Inserter<'c, 't>> {
        Inserter::new(tran)
    }

    /// Get an iterable over addresses in the database.
    ///
    /// # Example
    /// ```no_run
    /// use tools::Address;
    /// use deduplicator::db_hashes::*;
    ///
    /// use rusqlite::DropBehavior;
    ///
    /// let db = DbHashes::new("sqlite.db".into(), None).unwrap();
    /// let mut conn = db.get_conn().unwrap();
    ///
    /// let addresses: Vec<_> = db.get_addresses(&conn)
    ///     .unwrap()
    ///     .iter()
    ///     .unwrap()
    ///     .map(|addr| addr.unwrap())
    ///     .collect();
    /// ```
    pub fn get_addresses<'c>(
        &'c self,
        conn: &'c Connection,
    ) -> rusqlite::Result<AddressesIter<'c>> {
        AddressesIter::prepare(conn, &self.to_delete)
    }

    /// Get an iterable over hashes in the database that explicit a collision. The results are
    /// grouped by hash value.
    ///
    /// The result is partitioned into `nb_parts` partitions, the returned iterator will only
    /// browse results of the partition of index `part` (0 <= `part` < `nb_parts`).
    ///
    /// # Example
    /// ```no_run
    /// use tools::Address;
    /// use deduplicator::db_hashes::*;
    ///
    /// use rusqlite::DropBehavior;
    ///
    /// let db = DbHashes::new("sqlite.db".into(), None).unwrap();
    /// let mut conn = db.get_conn().unwrap();
    ///
    /// let hashes: Vec<_> = DbHashes::get_collisions_iter_for_parts(&conn, 0, 1)
    ///     .unwrap()
    ///     .iter()
    ///     .unwrap()
    ///     .map(|item| item.unwrap())
    ///     .collect();
    /// ```
    pub fn get_collisions_iter_for_parts(
        conn: &Connection,
        part: usize,
        nb_parts: usize,
    ) -> rusqlite::Result<CollisionsIter> {
        CollisionsIter::prepare(conn, part, nb_parts)
    }

    pub fn insert_to_delete(&mut self, to_delete: impl IntoIterator<Item = i64>) {
        self.to_delete.extend(to_delete)
    }

    /// Drop construction tables from the database. This will apply to the table containing hashes
    /// and the table containing addresses that have to be deleted.
    pub fn cleanup_database(&self) -> rusqlite::Result<()> {
        let conn = self.get_conn()?;
        conn.execute_batch(&format!("DROP TABLE {TABLE_HASHES};"))
    }

    /// Vacuum the database. Note that this function will have to be called after
    /// `cleanup_database` to effectively free some disk space.
    pub fn vacuum(&self) -> rusqlite::Result<()> {
        self.get_conn()?.execute_batch("VACUUM;")
    }
}

/// Materialize a transaction into a database that can be used to insert efficiently a bunch of
/// data.
pub struct Inserter<'c, 't> {
    tran: &'t Transaction<'c>,
    stmt_insert_address: Statement<'t>,
    stmt_insert_hash: Statement<'t>,
}

impl<'c, 't> Inserter<'c, 't> {
    /// Create a new inserter from a transaction.
    pub fn new(tran: &'t mut Transaction<'c>) -> rusqlite::Result<Self> {
        let stmt_insert_address = tran.prepare(&format!(
            "
                INSERT INTO {TABLE_ADDRESSES} (
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
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10);
            "
        ))?;

        let stmt_insert_hash = tran.prepare(&format!(
            "INSERT INTO {TABLE_HASHES} (address, hash) VALUES (?1, ?2);"
        ))?;

        Ok(Self {
            tran,
            stmt_insert_address,
            stmt_insert_hash,
        })
    }

    /// Insert an address into the database. A rank has to be passed which will be used to decide
    /// which address of a duplicated pair will be eliminated. When a duplicate is found, the
    /// address with a greater rank is kept.
    pub fn insert_address(&mut self, address: &Address, rank: f64) -> rusqlite::Result<i64> {
        self.stmt_insert_address.execute(&[
            &address.lat as &dyn ToSql,
            &address.lon,
            &address.number.as_ref().map(|s| s.as_str()),
            &address.street.as_ref().map(|s| s.as_str()),
            &address.unit.as_ref().map(|s| s.as_str()),
            &address.city.as_ref().map(|s| s.as_str()),
            &address.district.as_ref().map(|s| s.as_str()),
            &address.region.as_ref().map(|s| s.as_str()),
            &address.postcode.as_ref().map(|s| s.as_str()),
            &rank,
        ])?;
        Ok(self.tran.last_insert_rowid())
    }

    /// Insert the hash of an address into the database.
    pub fn insert_hash(&mut self, address_id: i64, address_hash: i64) -> rusqlite::Result<()> {
        self.stmt_insert_hash.execute([address_id, address_hash])?;
        Ok(())
    }
}

/// An iterable over the addresses of a database.
pub struct AddressesIter<'c> {
    stmt: Statement<'c>,
    skip: &'c HashSet<i64>,
}

impl<'c> AddressesIter<'c> {
    /// Request a connection for the list of addresses in the database.
    pub fn prepare(conn: &'c Connection, skip: &'c HashSet<i64>) -> rusqlite::Result<Self> {
        Ok(Self {
            stmt: conn.prepare(&format!("SELECT * FROM {TABLE_ADDRESSES};"))?,
            skip,
        })
    }

    /// Iterate over the list of result addresses.
    pub fn iter(
        &mut self,
    ) -> rusqlite::Result<impl Iterator<Item = rusqlite::Result<Address>> + '_> {
        let iter = self
            .stmt
            .query_map([], |row| Ok((row.get(0)?, row.try_into()?)))?
            .filter_map(|res| {
                let (id, addr) = match res {
                    Ok(x) => x,
                    Err(err) => return Some(Err(err)),
                };

                if self.skip.contains(&id) {
                    None
                } else {
                    Some(Ok(addr))
                }
            });

        Ok(iter)
    }
}

/// An address together with its hash.
#[derive(Debug, PartialEq)]
pub struct HashIterItem {
    pub address: Address,
    pub hash: i64,
    pub id: i64,
    pub rank: f64,
}

/// An iterable over addresses sorted by hash value in the database.
///
/// Note that as an address may have several hash values, it may appear several time (with a
/// separate hash value) in the iterations.
pub struct CollisionsIter<'c>(Statement<'c>);

impl<'c> CollisionsIter<'c> {
    /// Request the list of addresses ordered by hashes to a connection.
    pub fn prepare(conn: &'c Connection, part: usize, nb_parts: usize) -> rusqlite::Result<Self> {
        assert!(part < nb_parts);

        // Precompute bounds.
        // Note that these computations could be done by SQLite, however the query planner will
        // somehow forget that hashes are already sorted when we do so, resulting in computing an
        // unnecessary temporary B-Tree.
        let min_hash = conn
            .query_row(
                &format!("SELECT MIN(hash) FROM {};", TABLE_HASHES),
                [],
                |row: &rusqlite::Row| row.get(0),
            )
            .map_err(|err| teprintln!("[WARN] Could not read min hash value: `{}`", err))
            .unwrap_or(i64::MIN);

        let max_hash = conn
            .query_row(
                &format!("SELECT MAX(hash) FROM {};", TABLE_HASHES),
                [],
                |row: &rusqlite::Row| row.get(0),
            )
            .map_err(|err| teprintln!("[WARN] Could not read max hash value: `{}`", err))
            .unwrap_or(i64::MAX);

        let part = partition(min_hash..=max_hash, nb_parts)
            .nth(part)
            .expect("invalid partitionning");

        // Send the query
        let query = format!(
            "
                SELECT
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
                FROM {TABLE_HASHES} AS hash
                JOIN {TABLE_ADDRESSES} AS addr ON hash.address = addr.id
                WHERE (
                    hash.hash BETWEEN {start} AND {end}
                    AND EXISTS (
                        SELECT *
                        FROM {TABLE_HASHES}
                        WHERE hash = hash.hash AND address <> hash.address
                    )
                )
                ORDER BY hash.hash;
            ",
            start = part.start(),
            end = part.end(),
        );

        Ok(Self(conn.prepare(&query)?))
    }

    /// Iterate over the list of resulting hashes.
    pub fn iter(
        &mut self,
    ) -> rusqlite::Result<impl Iterator<Item = rusqlite::Result<HashIterItem>> + '_> {
        self.0.query_map([], |row| {
            Ok(HashIterItem {
                address: row.try_into()?,
                hash: row.get("hash")?,
                id: row.get("id")?,
                rank: row.get("rank")?,
            })
        })
    }
}
