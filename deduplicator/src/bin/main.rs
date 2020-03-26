use std::fs::{remove_file, File};
use std::path::PathBuf;

use libflate::gzip;
use structopt::StructOpt;
use tools::{tprintln, Address};

use deduplicator::{deduplicator::Deduplicator, sources::Source, utils::load_from_sqlite};

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
    openaddresses: Vec<PathBuf>,

    /// Path to data from OSM
    #[structopt(long)]
    osm: Vec<PathBuf>,

    /// Path to data from Bano as an SQLite database
    #[structopt(long)]
    bano_db: Vec<PathBuf>,

    /// Path to data from OpenAddress as an SQLite database
    #[structopt(long)]
    openaddresses_db: Vec<PathBuf>,

    /// Path to data from OSM as an SQLite database
    #[structopt(long)]
    osm_db: Vec<PathBuf>,

    /// Path for output database.
    #[structopt(long, default_value = "addresses.db")]
    output_db: PathBuf,

    /// Keep construction tables and the output database
    #[structopt(short, long)]
    keep: bool,

    /// Output database as an OpenAddress-like gzip CSV file
    #[structopt(short, long, default_value = "deduplicated.csv.gz")]
    output_csv: PathBuf,

    /// Number of pages to be used by SQLite (one page is 4096 bytes)
    #[structopt(short, long, default_value = "10000")]
    cache_size: u32,
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
                .openaddresses_db
                .into_iter()
                .map(|s| (Source::OpenAddress, s)),
        );

    let raw_sources = None
        .into_iter()
        .chain(params.bano.into_iter().map(|s| (Source::Bano, s)))
        .chain(params.osm.into_iter().map(|s| (Source::Osm, s)))
        .chain(
            params
                .openaddresses
                .into_iter()
                .map(|s| (Source::OpenAddress, s)),
        );

    // Load from all sources

    let mut deduplication = Deduplicator::new(params.output_db.clone(), Some(params.cache_size))?;

    for (source, path) in db_sources {
        tprintln!("Loading {:?} addresses from database {:?}...", source, path);

        load_from_sqlite(
            &mut deduplication,
            path,
            move |addr| source.filter(&addr),
            move |addr| source.ranking(&addr),
        )?;
    }

    for (source, path) in raw_sources {
        tprintln!("Loading {:?} addresses from path {:?}...", source, path);

        let filter = move |addr: &Address| source.filter(&addr);
        let ranking = move |addr: &Address| source.ranking(&addr);
        let import_method = match source {
            Source::Osm => importer_osm::import_addresses,
            Source::OpenAddress => importer_openaddresses::import_addresses,
            Source::Bano => importer_bano::import_addresses,
        };

        import_method(&path, &mut deduplication.get_db_inserter(filter, ranking)?);
    }

    // --- Apply deduplication

    tprintln!("Deduplication...");
    deduplication.compute_duplicates()?;

    tprintln!("Cleaning...");
    deduplication.apply_deletions()?;

    // --- Dump CSV

    tprintln!("Write compressed CSV...");
    let file = File::create(params.output_csv).expect("failed to create dump file");
    let mut encoder = gzip::Encoder::new(file).expect("failed to init gzip encoder");
    deduplication.openaddresses_dump(&mut encoder)?;
    encoder.finish().as_result().expect("failed to end dump");

    // --- Cleanup

    if !&params.keep {
        remove_file(&params.output_db)
            .map_err(|_| eprintln!(r"/!\ failed to remove the working database file"))
            .ok();
    }

    Ok(())
}
