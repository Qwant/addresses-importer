# OpenStreetMap importer

This importer import the addresses from [OpenStreetMap]. It requires as first argument, the `.pbf` file file from which you want to extract the addresses.

## How it works

It'll filter out everything which isn't a node and then check on a node if it has tags starting with "addr:". If so, it'll then read the tags from it and try to extract all the address information it can. Once done, it'll insert the data inside the database (or fail if too much information is missing for that node).

You can run it like this:

```bash
$ cargo run --release -- [the PBF file]
```

[OpenStreetMap]: https://openstreetmap.org
