use lsm_storage_engine::Engine;
use std::fs;
use std::path::Path;

fn main() -> std::io::Result<()> {
    let data_dir = "./data_example";

    // Cleanup previous runs if necessary
    if Path::new(data_dir).exists() {
        fs::remove_dir_all(data_dir)?;
    }

    // Open the engine (directory, max_memtable_size)
    // We use a small memtable size (1KiB) to demonstrate flushing/compaction easily in a small example
    let engine = Engine::open(data_dir, 1024)?;

    println!("Inserting data...");
    // Insert/Update
    engine.put(b"user:1".to_vec(), b"Albiere".to_vec())?;
    engine.put(b"user:2".to_vec(), b"Antigravity".to_vec())?;

    // Retrieve
    if let Some(val) = engine.get(b"user:1")? {
        println!("Retrieved user:1 -> {}", String::from_utf8(val).unwrap());
    }

    println!("Deleting user:2...");
    // Delete (inserts a tombstone)
    engine.delete(b"user:2".to_vec())?;

    if engine.get(b"user:2")?.is_none() {
        println!("Successfully deleted user:2 (not found)");
    }

    println!("Example completed successfully!");

    Ok(())
}
