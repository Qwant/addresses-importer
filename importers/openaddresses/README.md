# OpenAddresses importer

This importer import the addresses from [OpenAddresses]. It requires as first argument, the folder
in which it'll find the data from [OpenAddresses] and runs as follows:

```bash
$ cargo run --release -- [folder where you extracted OpenAddresses data]
```

The generated database has two tables which look like this:

```sql
CREATE TABLE IF NOT EXISTS addresses(
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

## Using it as a library

You can use this importer as a library/dependency directly. The entry point is:

```rust
fn import_addresses<P: AsRef<Path>>(
    db_file_name: &str,
    folder: P,
    remove_db_data: bool,
) -> DB;
```

The arguments are:

 * `db_file_name`: where the sqlite database will be stored
 * `folder`: where the [OpenAddresses] data is located
 * `remove_db_data`: if `false` and if a `db_file_name` already exists, it won't be removed nor
   overwritten.

It returns a `DB` objects. Please take a look at the generated documentation for more information
about it. You can generate the documentation with this command:

```bash
$ cargo doc
$ cargo doc --open # if you want to take a look at the generated documentation
```

[OpenAddresses]: https://openaddresses.io/
