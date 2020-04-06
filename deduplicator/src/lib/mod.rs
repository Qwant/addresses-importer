//! Utilities to deduplicate pair of addresses inside of a large database.

extern crate crossbeam_channel;
extern crate csv;
extern crate geo;
extern crate geo_geojson;
extern crate importer_bano;
extern crate importer_openaddresses;
extern crate importer_osm;
#[macro_use]
extern crate tools;
extern crate itertools;
extern crate libsqlite3_sys;
extern crate num_cpus;
extern crate once_cell;
extern crate prog_rs;
extern crate rpostal;
extern crate rusqlite;
extern crate structopt;

pub mod db_hashes;
pub mod dedupe;
pub mod deduplicator;
pub mod sources;
pub mod utils;

#[cfg(test)]
mod tests;
