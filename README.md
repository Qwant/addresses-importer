# addresses-importer

The goal of this project is to aggregate multiple sources of addresses and then merge them into one. Currently we're using [OpenAddresses](https://openaddresses.io/) and [OpenStreetMap](https://www.openstreetmap.org).

The big part of this project being the deduplication process and cleaning the data.

## Importers

Each importer generates an [sqlite](https://www.sqlite.org/index.html) `addresses.db` file with the following tables:

```
addresses(
  lat REAL NOT NULL,
  lon REAL NOT NULL,
  number TEXT,
  street TEXT NOT NULL,
  unit TEXT,
  city TEXT,
  district TEXT,
  region TEXT,
  postcode TEXT,
  PRIMARY KEY (lat, lon, number, street, city, place)
)

addresses_errors(
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
)
```

The `addresses_errors` table is only used as a way to improve the data gathering that'll be put in the first table.
