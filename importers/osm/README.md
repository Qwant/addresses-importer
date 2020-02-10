# OpenStreetMap importer

This importer import the addresses from [OpenStreetMap]. It requires as first argument, the `.pbf`
file file from which you want to extract the addresses.

## How it works

It runs through all the elements and keeps them as follows:

 * If it's a `node`, it needs to have both "addr:housenumber" and "addr:street" tags.
 * If it's a `way`, it also needs to have both "addr:housenumber" and "addr:street" tags but it also needs to have at least one `node`, otherwise we can't determine its location (each node has an associated latitude/longitude, which isn't the case for a way).
 * If it's a `relation`, it needs a tag "name" and at least one element with the tag "type" with "associatedStreet" as value.

Once we have gathered all the elements that might match our needs, we transform this data as addresses. Just like previously, the treatment different depending on the type of the element:

 * If it's a `node`, we gather the tags and the position to generate the address.
 * If it's a `way`, we generate a polygon from its node and use its centroid's location as the way's location. Then it's the same as a `node`: we gather the tags and the position to generate the address.
 * If it's a `relation`, it gets a bit more tricky since it means we might have multiple addresses. So we iterate through the children:
   * If the child is a `node` and it has a "addr:housenumber" tag, we generate a new address by using most of its tags except for the street name (which is the one from the `relation`).
   * If the child is a `way` and it has a "addr:housenumber" tag, we use the same method as we described above for a `way`, except we replace the street name (if there is any) by the one in the parent `relation`.
   * If the child is a `relation`, we currently ignore it.

## Running it

You can run it like this:

```bash
$ cargo run --release -- [the PBF file]
```

The generated database has two tables. Take a look at the `tools` folder's README to see what it looks like.

## Using it as a library

You can use this importer as a library/dependency directly. The entry point is:

```rust
fn import_addresses<P: AsRef<Path>, T: CompatibleDB>(
    folder: P,
    db: &mut T,
);
```

The arguments are:

 * `pbf_file`: where the `.pdf` [OpenStreetMap] data file is located
 * `db`: an object implementing `tools::CompatibleDB`

You can generate the documentation with this command:

```bash
$ cargo doc
$ cargo doc --open # if you want to take a look at the generated documentation
```

[OpenStreetMap]: https://openstreetmap.org
