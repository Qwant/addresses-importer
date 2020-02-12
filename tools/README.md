# Tools

This crate is used as a dependency to prevent code duplication and enforce data formatting. It can
only be used as a library, it doesn't generate a binary. The main things it provides are:

 * `CompatibleDB` trait, used for importers to be sure they all generate data in the same format.
 * `Address` struct, used to store the addresses through the `CompatibleDB` trait.
 * `tprint` and `teprint` macros: they do the same as `println` and `eprintln` but prepend the message with the current hour. Very useful for logging.
 * `DB` struct, which is the default type used for importers. It implements the `CompatibleDB` trait.

The `DB` struct can be used as a default option to store addresses, when using it all addresses are
stored in sqlite databases looking like this:

```sql
CREATE TABLE IF NOT EXISTS addresses(
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
);
CREATE TABLE IF NOT EXISTS addresses_errors(
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
);
```

The `addresses_errors` table is used to store the error and the data that generated this error.
It's mostly because the "NOT NULL" constraints aren't respected, but sometimes it's also because
of duplicates (very rarely though).
