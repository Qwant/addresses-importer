use std::env;
use tools::{self, CompatibleDB, DB};

fn main() {
    let args = env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        eprintln!("Expected PBF file path");
    }
    let mut db = DB::new("addresses.db", 1000, true).expect("Failed to create DB");
    osm::import_addresses(&args[1], &mut db);
    println!(
        "Got {} addresses in {} cities (and {} errors)",
        db.get_nb_addresses(),
        db.get_nb_cities(),
        db.get_nb_errors(),
    );
    println!("Errors by categories:");
    let rows = db.get_nb_by_errors_kind();
    for (kind, nb) in rows {
        println!("  {} => {} occurences", kind, nb);
    }
}
