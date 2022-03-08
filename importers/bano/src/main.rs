use std::env;
use tools::{teprintln, tprintln, CompatibleDB, DB};

fn main() {
    let args = env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        teprintln!("Expected bano csv file");
        return;
    }

    let mut db = DB::new("addresses.db", 10000, true).expect("failed to create DB");
    bano::import_addresses(&args[1], &mut db);

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
