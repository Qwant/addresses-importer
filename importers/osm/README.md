# OpenStreetMap importer

This importer import the addresses from [OpenStreetMap]. It requires as first argument, the `.pbf`
file file from which you want to extract the addresses.

## How it works

It runs through 2 passes:

The first pass collects all ways containing at least one node and the tags "addr:housenumber"
and "addr:street" and all the nodes (but we remove all the tags that we have no interest in, so all
those which don't start with "addr:") and store them inside a sqlite database (to prevent too high
RAM usage).

Why keeping all the nodes you might wonder? Because we might need them for a way. The ways don't
have a position, so we generate a polygon using [geos](https://github.com/georust/geos/) and use
its centroid as the way's position.

Second pass: now that we have all these nodes and ways stored inside a database, we iterate through
them. Small note on the nodes: even though we stored them previously without filtering, we do filter
them on this pass for the simple reason that if they don't have a house number, it'll generate an
SQL error which would be useless.

## Running it

You can run it like this:

```bash
$ cargo run --release -- [the PBF file]
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
    pbf_file: P,
    remove_db_data: bool,
) -> DB;
```

The arguments are:

 * `db_file_name`: where the sqlite database will be stored
 * `pbf_file`: where the `.pdf` [OpenStreetMap] data file is located
 * `remove_db_data`: if `false` and if a `db_file_name` already exists, it won't be removed nor
   overwritten.

It returns a `DB` objects. Please take a look at the generated documentation for more information
about it. You can generate the documentation with this command:

```bash
$ cargo doc
$ cargo doc --open # if you want to take a look at the generated documentation
```

[OpenStreetMap]: https://openstreetmap.org
