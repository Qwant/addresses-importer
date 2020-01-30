use std::env;
use tools::{CompatibleDB, DB};

fn main() {
    let args = env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        eprintln!("Expected openaddresses folder");
        return;
    }

    let mut db = DB::new("addresses.db", 10000, true).expect("failed to create DB");
    openaddresses::import_addresses(&args[1], &mut db);

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
