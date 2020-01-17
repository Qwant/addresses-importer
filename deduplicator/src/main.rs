extern crate rpostal;
extern crate rusqlite;
extern crate structopt;

mod address;
mod dedupe;

use std::path::PathBuf;

use rusqlite::{Connection, NO_PARAMS};
use structopt::StructOpt;

use address::Address;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "deduplicator",
    about = "Deduplicate addresses from several sources."
)]
struct Params {
    /// Path to data from various sources.
    #[structopt(short, long)]
    sources: Vec<PathBuf>,

    /// Path to output database.
    #[structopt(short, long, default_value = "addresses.db")]
    output: PathBuf,
}

fn main() -> rusqlite::Result<()> {
    let params = Params::from_args();

    let mut deduplication = dedupe::Dedupe::new(&params.output)?;

    for path in &params.sources {
        let input_conn = Connection::open(path)?;
        let mut stmt = input_conn.prepare("SELECT * FROM addresses")?;

        let addresses = stmt
            .query_map(NO_PARAMS, |row| {
                Ok(Address {
                    lat: row.get("lat")?,
                    lon: row.get("lon")?,
                    number: row.get("number")?,
                    street: row.get("street")?,
                    unit: row.get("unit")?,
                    city: row.get("city")?,
                    district: row.get("district")?,
                    region: row.get("region")?,
                    postcode: row.get("postcode")?,
                })
            })?
            .map(|addr| {
                addr.unwrap_or_else(|e| panic!("failed to read address from source: {}", e))
            });

        deduplication.load_addresses(addresses)?;
    }

    deduplication.remove_duplicate()?;

    println!("{:?}", params);
    Ok(())
}
