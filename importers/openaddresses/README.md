# OpenAddresses importer

This is the importer for [OpenAddresses]. It requires as first argument, the folder in which it'll
find the data from [OpenAddresses].

## How it works

The script goes through all folder and sub-folders, reading all the CSV files available. Each line is then added if it has the following elements:

 * longitude
 * latitude
 * street name
 * house number

## Running it

You can run it like this:

```bash
$ cargo run --release -- [folder where you extracted OpenAddresses data]
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

 * `folder`: where the [OpenAddresses] data is located
 * `db`: an object implementing `tools::CompatibleDB`

You can generate the documentation with this command:

```bash
$ cargo doc
$ cargo doc --open # if you want to take a look at the generated documentation
```

[OpenAddresses]: https://openaddresses.io/
