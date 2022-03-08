use std::fs::{remove_file, File};
use std::path::PathBuf;
use std::time::Duration;

use libflate::gzip;
use structopt::StructOpt;
use tools::{teprintln, tprintln, Address};

use deduplicator::{
    deduplicator::{DedupeConfig, Deduplicator},
    sources::Source,
    utils::{load_from_sqlite, parse_duration},
};

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

    /// While not explicitly disabled with this flag, addresses from france are
    /// only imported from BANO sources.
    #[structopt(long)]
    skip_source_filters: bool,

    /// Path for output database.
    #[structopt(long, default_value = "addresses.db")]
    output_db: PathBuf,

    /// Keep construction tables and the output database both at the start and the end of the
    /// deduplication.
    #[structopt(short, long)]
    keep: bool,

    /// Output database as an OpenAddress-like gzip CSV file
    #[structopt(
        short,
        long = "output-compressed-csv",
        default_value = "deduplicated.csv.gz"
    )]
    output_csv: PathBuf,

    /// Number of pages to be used by SQLite (one page is 4096 bytes)
    #[structopt(short, long, default_value = "10000")]
    cache_size: u32,

    /// Number of thread to target during the computation.
    #[structopt(short, long)]
    num_threads: Option<usize>,

    /// Redraw delay for displayed progress (in ms)
    #[structopt(long, default_value = "1000", parse(try_from_str = parse_duration))]
    refresh_delay: Duration,
}

impl Params {
    fn cleanup_empty_paths(mut self) -> Self {
        for source in [&mut self.bano, &mut self.openaddresses, &mut self.osm] {
            source.retain(|path| !path.as_os_str().is_empty());
        }

        self
    }
}

fn main() -> rusqlite::Result<()> {
    // --- Read parameters

    let params = Params::from_args().cleanup_empty_paths();

    if !params.keep {
        remove_file(&params.output_db)
            .map(|()| teprintln!("Removed {:?}", params.output_db))
            .map_err(|err| match err.kind() {
                std::io::ErrorKind::NotFound => {}
                _ => teprintln!("Failed to remove {:?} file: {:?}", params.output_db, err),
            })
            .ok();
    }

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

    let dedupe_config = DedupeConfig {
        refresh_delay: params.refresh_delay,
        nb_threads: params.num_threads.unwrap_or_else(num_cpus::get),
    };

    let mut deduplication = Deduplicator::new(
        params.output_db.clone(),
        dedupe_config,
        Some(params.cache_size),
    )?;

    for (source, path) in db_sources {
        tprintln!("Loading {:?} addresses from database {:?}...", source, path);

        load_from_sqlite(
            &mut deduplication,
            path,
            move |addr| source.filter(addr),
            move |addr| source.ranking(addr),
            params.refresh_delay,
        )?;
    }

    for (source, path) in raw_sources {
        tprintln!("Loading {:?} addresses from path {:?}...", source, path);

        let skip_source_filters = params.skip_source_filters;
        let filter = move |addr: &Address| skip_source_filters || source.filter(addr);
        let ranking = move |addr: &Address| source.ranking(addr);
        let mut db = deduplication.get_db_inserter(filter, ranking)?;

        match source {
            Source::Osm => importer_osm::import_addresses(&path, &mut db),
            Source::OpenAddress => importer_openaddresses::import_addresses(path, &mut db),
            Source::Bano => importer_bano::import_addresses(path, &mut db),
        }
    }

    // --- Apply deduplication

    tprintln!("Deduplication...");
    deduplication.compute_duplicates()?;

    // --- Dump CSV

    tprintln!("Write compressed CSV...");
    let file = File::create(params.output_csv).expect("failed to create dump file");
    let mut encoder = gzip::Encoder::new(file).expect("failed to init gzip encoder");
    deduplication.openaddresses_dump(&mut encoder)?;
    encoder.finish().as_result().expect("failed to end dump");

    // --- Cleanup

    if !params.keep {
        remove_file(&params.output_db)
            .map(|()| teprintln!("Removed {:?}", params.output_db))
            .map_err(|err| teprintln!("Failed to remove {:?}: {:?}", params.output_db, err))
            .ok();
    }

    Ok(())
}
