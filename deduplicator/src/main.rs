extern crate crossbeam_channel;
extern crate csv;
extern crate geo;
extern crate geo_geojson;
extern crate importer_bano;
extern crate importer_openaddress;
extern crate importer_osm;
extern crate importer_tools;
extern crate itertools;
#[macro_use]
extern crate lazy_static;
extern crate libsqlite3_sys;
extern crate num_cpus;
extern crate prog_rs;
extern crate rpostal;
extern crate rusqlite;
extern crate structopt;

#[cfg(test)]
mod tests;

mod db_hashes;
mod dedupe;
mod deduplicator;
mod sources;
mod utils;

use std::fs::File;
use std::path::PathBuf;

use importer_tools::Address;
use libflate::gzip;
use structopt::StructOpt;

use deduplicator::Deduplicator;
use sources::Source;
use utils::load_from_sqlite;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "deduplicator",
    about = "Deduplicate addresses from several sources."
)]
struct Params {
    /// Path to data from bano
    #[structopt(long)]
    bano: Vec<PathBuf>,

    /// Path to data from OpenAddress
    #[structopt(long)]
    openaddress: Vec<PathBuf>,

    /// Path to data from OSM
    #[structopt(long)]
    osm: Vec<PathBuf>,

    /// Path to data from Bano as an SQLite database
    #[structopt(long)]
    bano_db: Vec<PathBuf>,

    /// Path to data from OpenAddress as an SQLite database
    #[structopt(long)]
    openaddress_db: Vec<PathBuf>,

    /// Path to data from OSM as an SQLite database
    #[structopt(long)]
    osm_db: Vec<PathBuf>,

    /// Path for output database.
    #[structopt(short, long, default_value = "addresses.db")]
    output_db: PathBuf,

    /// Keep construction tables in the output database
    #[structopt(short, long)]
    keep: bool,

    /// Output database as an OpenAddress-like CSV file
    #[structopt(long)]
    output_csv: Option<PathBuf>,

    /// Output database as an OpenAddress-like gzip CSV file
    #[structopt(short, long)]
    output_compressed_csv: Option<PathBuf>,
}

fn main() -> rusqlite::Result<()> {
    // --- Read parameters

    let params = Params::from_args();

    let db_sources = None
        .into_iter()
        .chain(params.bano_db.into_iter().map(|s| (Source::Bano, s)))
        .chain(params.osm_db.into_iter().map(|s| (Source::Osm, s)))
        .chain(
            params
                .openaddress_db
                .into_iter()
                .map(|s| (Source::OpenAddress, s)),
        );

    let raw_sources = None
        .into_iter()
        .chain(params.bano.into_iter().map(|s| (Source::Bano, s)))
        .chain(params.osm.into_iter().map(|s| (Source::Osm, s)))
        .chain(
            params
                .openaddress
                .into_iter()
                .map(|s| (Source::OpenAddress, s)),
        );

    // Load from all sources

    let mut deduplication = Deduplicator::new(params.output_db)?;

    for (source, path) in db_sources {
        println!("Loading {:?} addresses from database {:?}", source, path);

        load_from_sqlite(
            &mut deduplication,
            path,
            move |addr| source.filter(&addr),
            move |addr| source.ranking(&addr),
        )?;
    }

    for (source, path) in raw_sources {
        println!("Loading {:?} addresses from path {:?}", source, path);

        let filter = move |addr: &Address| source.filter(&addr);
        let ranking = move |addr: &Address| source.ranking(&addr);
        let import_method = match source {
            Source::Osm => importer_osm::import_addresses,
            Source::OpenAddress => importer_openaddress::import_addresses,
            Source::Bano => importer_bano::import_addresses,
        };

        import_method(&path, &mut deduplication.get_db_inserter(filter, ranking)?);
    }

    // --- Apply deduplication

    deduplication.compute_duplicates()?;
    deduplication.apply_and_clean(params.keep)?;

    // --- Dump CSV

    if let Some(output_csv) = params.output_csv {
        println!("Write CSV");
        let file = File::create(output_csv).expect("failed to create dump file");
        deduplication.openaddress_dump(file)?;
    }

    if let Some(compressed_csv) = params.output_compressed_csv {
        println!("Write compressed CSV");
        let file = File::create(compressed_csv).expect("failed to create dump file");
        let mut encoder = gzip::Encoder::new(file).expect("failed to init gzip encoder");
        deduplication.openaddress_dump(&mut encoder)?;
        encoder.finish().as_result().expect("failed to end dump");
    }

    Ok(())
}
