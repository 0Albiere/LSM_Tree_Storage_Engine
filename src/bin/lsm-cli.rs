use lsm_storage_engine::{SSTable, Entry};
use std::env;
use std::path::Path;

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    let command = &args[1];
    match command.as_str() {
        "sst-dump" => {
            if args.len() < 3 {
                println!("Usage: lsm-cli sst-dump <path>");
                return Ok(());
            }
            dump_sstable(&args[2])?;
        }
        "sst-verify" => {
            if args.len() < 3 {
                println!("Usage: lsm-cli sst-verify <path>");
                return Ok(());
            }
            verify_sstable(&args[2])?;
        }
        "compact" => {
            if args.len() < 3 {
                println!("Usage: lsm-cli compact <data_dir>");
                return Ok(());
            }
            manual_compaction(&args[2])?;
        }
        _ => {
            println!("Unknown command: {}", command);
            print_usage();
        }
    }

    Ok(())
}

fn print_usage() {
    println!("LSM-Tree Storage Engine CLI");
    println!("Usage:");
    println!("  lsm-cli sst-dump <path>    - Dump metadata and records from an SSTable");
    println!("  lsm-cli sst-verify <path>  - Verify the checksum of an SSTable");
    println!("  lsm-cli compact <data_dir> - Manually trigger compaction on all SSTables in a directory");
}

fn manual_compaction(dir: &str) -> std::io::Result<()> {
    println!("Manually triggering compaction for: {}", dir);
    let engine = lsm_storage_engine::Engine::open(dir, 1024 * 1024)?; // default 1MB memtable for recovery
    engine.compact()?;
    println!("Compaction completed successfully.");
    Ok(())
}

fn dump_sstable(path: &str) -> std::io::Result<()> {
    println!("Dumping SSTable: {}", path);
    if !Path::new(path).exists() {
        println!("Error: File not found");
        return Ok(());
    }

    let sst = SSTable::open(path)?;
    println!("--- Metadata ---");
    println!("Path: {:?}", sst.path());
    
    println!("--- Records ---");
    let iter = sst.iter()?;
    let mut count = 0;
    for result in iter {
        let (key, entry) = result?;
        match entry {
            Entry::Value(v) => {
                println!("  Key: {:?} | Value: {:?} ({} bytes)", 
                    String::from_utf8_lossy(&key), 
                    String::from_utf8_lossy(&v),
                    v.len());
            }
            Entry::Tombstone => {
                println!("  Key: {:?} | [TOMBSTONE]", String::from_utf8_lossy(&key));
            }
        }
        count += 1;
    }
    println!("Total records: {}", count);
    Ok(())
}

fn verify_sstable(path: &str) -> std::io::Result<()> {
    println!("Verifying SSTable: {}", path);
    match SSTable::open(path) {
        Ok(_) => println!("Checksum verification: PASSED"),
        Err(e) => println!("Checksum verification: FAILED ({})", e),
    }
    Ok(())
}
