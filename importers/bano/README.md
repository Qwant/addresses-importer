# BANO importer

This is the importer for [BANO]. It requires as first argument, the csv file (you can
download it from [here](http://bano.openstreetmap.fr/data/)) in which it'll find the data from
[BANO].

## How it works

The script reads the provided CSV file. Each line is then added if it has the following elements:

 * longitude
 * latitude
 * street name
 * house number

## Running it

You can run it like this:

```bash
$ cargo run --release -- [BANO csv file]
```

The result will be available into the `addresses.db` file.

## Using it as a library

You can use this importer as a library/dependency directly. The entry point is:

```rust
fn import_addresses<P: AsRef<Path>, T: CompatibleDB>(
    file_path: P,
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
