use std::env;
use tools::{self, teprintln, tprintln, CompatibleDB, DB};

fn main() {
    let args = env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        eprintln!("Expected PBF file path");
        return;
    }
    let mut db = DB::new("addresses.db", 1000, true).expect("Failed to create DB");
    osm::import_addresses(args[1].as_ref(), &mut db);
    tprintln!(
        "Got {} addresses (and {} errors)",
        db.get_nb_addresses(),
        db.get_nb_errors(),
    );

    teprintln!("Errors by categories:");
    let rows = db.get_nb_by_errors_kind();
    for (kind, nb) in rows {
        teprintln!("  {} => {} occurences", kind, nb);
    }
}
