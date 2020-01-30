extern crate crossbeam_channel;
extern crate geo;
extern crate geo_geojson;
extern crate importer_tools;
extern crate itertools;
extern crate num_cpus;
#[macro_use]
extern crate lazy_static;
extern crate importer_openaddress;
extern crate importer_osm;
extern crate libsqlite3_sys;
extern crate prog_rs;
extern crate rpostal;
extern crate rusqlite;
extern crate structopt;

#[cfg(test)]
mod tests;

mod db_hashes;
mod dedupe;
mod utils;

use std::path::PathBuf;

use geo::algorithm::contains::Contains;
use geo::{MultiPolygon, Point};
use importer_tools::Address;
use structopt::StructOpt;

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
    /// Path to data from OpenAddress
    #[structopt(long)]
    openaddress: Vec<PathBuf>,

    /// Path to data from OSM
    #[structopt(long)]
    osm: Vec<PathBuf>,

    /// Path to data from OpenAddress as an SQLite database
    #[structopt(long)]
    openaddress_db: Vec<PathBuf>,

    /// Path to data from OSM as an SQLite database
    #[structopt(long)]
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

    // --- Read database from OSM

    let osm_filter = move |addr: &Address| !is_in_france(addr);
    let osm_ranking = |addr: &Address| {
        PRIORITY_OSM + addr.count_non_empty_fields() as f64 / (1. + Address::NB_FIELDS as f64)
    };

    for path in params.osm_db {
        println!("Read OSM from database: {:?}", &path);
        load_from_sqlite(&mut deduplication, path, osm_filter.clone(), osm_ranking)
            .expect("failed to load OSM from database");
    }

    for path in params.osm {
        println!("Read raw OSM from path: {:?}", &path);
        importer_osm::import_addresses(
            path,
            &mut deduplication.get_db_inserter(osm_filter.clone(), osm_ranking)?,
        );
    }

    // -- Read database from OpenAddress

    let openaddress_filter = |_addr: &Address| true;
    let openaddress_ranking = |addr: &Address| {
        PRIORITY_OPENADDRESS
            + addr.count_non_empty_fields() as f64 / (1. + Address::NB_FIELDS as f64)
    };

    for path in params.openaddress_db {
        load_from_sqlite(
            &mut deduplication,
            path,
            openaddress_filter,
            openaddress_ranking,
        )
        .expect("failed to load OpenAddress from database");
    }

    for path in params.openaddress {
        println!("Read raw OpenAddress from path: {:?}", &path);
        importer_openaddress::import_addresses(
            &path,
            &mut deduplication.get_db_inserter(openaddress_filter, openaddress_ranking)?,
        );
    }

    // --- Apply deduplication

    deduplication.compute_duplicates()?;
    deduplication.apply_and_clean()?;

    Ok(())
}
