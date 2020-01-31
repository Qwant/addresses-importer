# BANO importer

This importer import the addresses from [BANO]. It requires as first argument, the csv file (you can
download it from [here](http://bano.openstreetmap.fr/data/)) in which it'll find the data from
[BANO] and runs as follows:

```bash
$ cargo run --release -- [BANO csv file]
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
fn import_addresses<P: AsRef<Path>, T: CompatibleDB>(
    folder: P,
    db: &mut T,
);
```

The arguments are:

 * `file`: [BANO csv file]
 * `db`: an object implementing `tools::CompatibleDB`

You can generate the documentation with this command:

```bash
$ cargo doc
$ cargo doc --open # if you want to take a look at the generated documentation
```

[BANO]: https://www.data.gouv.fr/fr/datasets/base-d-adresses-nationale-ouverte-bano/
