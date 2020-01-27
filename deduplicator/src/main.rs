extern crate crossbeam_channel;
extern crate geo;
extern crate geo_geojson;
extern crate num_cpus;
#[macro_use]
extern crate lazy_static;
extern crate libsqlite3_sys;
extern crate prog_rs;
extern crate rpostal;
extern crate rusqlite;
extern crate structopt;

#[macro_use]
mod address;
mod db_hashes;
mod dedupe;
mod tests;
mod utils;

use std::path::PathBuf;

use geo::algorithm::contains::Contains;
use geo::{MultiPolygon, Point};
use structopt::StructOpt;

use address::Address;
use dedupe::Dedupe;
use utils::load_from_sqlite;

const FRANCE_GEOJSON: &str = include_str!("data/france.json");

const PRIORITY_OSM: f64 = 2.;
const PRIORITY_OPENADDRESS: f64 = 1.;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "deduplicator",
    about = "Deduplicate addresses from several sources."
)]
struct Params {
    /// Path to data from OpenAddress as an SQLite database
    #[structopt(short = "a", long)]
    openaddress_db: Vec<PathBuf>,

    /// Path to data from OSM as an SQLite database
    #[structopt(short = "s", long)]
    osm_db: Vec<PathBuf>,

    /// Path to output database.
    #[structopt(short, long, default_value = "addresses.db")]
    output: PathBuf,
}

fn main() -> rusqlite::Result<()> {
    // --- Load France filter

    let france_shape: MultiPolygon<_> = {
        let collection =
            geo_geojson::from_str(FRANCE_GEOJSON).expect("failed to parse shape for France");
        collection
            .into_iter()
            .next()
            .expect("found an empty collection for France")
            .into_multi_polygon()
            .expect("France should be a MultiPolygon")
    };

    let is_in_france = move |address: &Address| -> bool {
        france_shape.contains(&Point::new(address.lon, address.lat))
    };

    // --- Read parameters

    let params = Params::from_args();
    let mut deduplication = Dedupe::new(params.output)?;

    // --- Read database from various sources

    println!(
        "Loading OSM addresses from {} SQLite databases",
        params.osm_db.len()
    );

    for path in params.osm_db {
        load_from_sqlite(
            &mut deduplication,
            path,
            |addr| !is_in_france(addr),
            |addr| {
                PRIORITY_OSM
                    + addr.count_non_empty_fields() as f64 / (1. + Address::NB_FIELDS as f64)
            },
        )
        .expect("failed to load addresses from database");
    }

    println!(
        "Loading OpenAddress addresses from {} SQLite databases",
        params.openaddress_db.len()
    );

    for path in params.openaddress_db {
        load_from_sqlite(
            &mut deduplication,
            path,
            |_| true,
            |addr| {
                PRIORITY_OPENADDRESS
                    + addr.count_non_empty_fields() as f64 / (1. + Address::NB_FIELDS as f64)
            },
        )
        .expect("failed to load addresses from database");
    }

    // --- Apply deduplication

    deduplication.compute_duplicates()?;
    deduplication.apply_and_clean()?;

    Ok(())
}
