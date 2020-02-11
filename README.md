# addresses-importer

The goal of this project is to aggregate multiple sources of addresses and then merge them into one. Currently we're using [OpenAddresses](https://openaddresses.io/) and [OpenStreetMap](https://www.openstreetmap.org).

The big part of this project being the deduplication process and cleaning the data.

## Workflow

It first loads addresses data using the importers. You might want to take a look in the `importers` folder if you want more information on a specific importer.

To make sure they generate the same kind of data, we wrote a trait called `CompatibleDB` which is available in `tools/src/lib.rs` alongside an `Address` type. Therefore, the importers are forced to all provide the same information in the same format. It's then up to the caller to implement them however they want.

Once the imports are done, all the data is merged into one big file. However, a same address may have been imported several times from different sources and sometime several time in the same source. This is where the `[deduplicator](./deduplicator)` comes in. As usual, more information can be found in its README file.
