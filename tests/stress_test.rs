use lsm_storage_engine::Engine;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::SystemTime;

fn setup_test_dir(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("lsm_stress_{}_{}", name, SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn test_concurrent_write_read_stress() {
    let dir = setup_test_dir("concurrent_stress");
    let engine = Arc::new(Engine::open(&dir, 64 * 1024).unwrap()); // 64KB memtable to trigger frequent flushes
    
    let num_writers = 4;
    let num_readers = 4;
    let items_per_writer = 1000;
    
    let barrier = Arc::new(Barrier::new(num_writers + num_readers));
    let mut handles = Vec::new();

    // Spawn writers
    for w in 0..num_writers {
        let e = Arc::clone(&engine);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            for i in 0..items_per_writer {
                let key = format!("writer_{}_key_{}", w, i).into_bytes();
                let val = format!("value_{}", i).into_bytes();
                e.put(key, val).unwrap();
            }
        }));
    }

    // Spawn readers
    for r in 0..num_readers {
        let e = Arc::clone(&engine);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            let mut found = 0;
            for i in 0..(items_per_writer * num_writers) {
                let key = format!("writer_{}_key_{}", i % num_writers, i / num_writers).into_bytes();
                if let Ok(Some(_)) = e.get(&key) {
                    found += 1;
                }
            }
            println!("Reader {} found {} items during stress", r, found);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Final verification
    println!("Final verification...");
    for w in 0..num_writers {
        for i in 0..items_per_writer {
            let key = format!("writer_{}_key_{}", w, i).into_bytes();
            let expected = format!("value_{}", i).into_bytes();
            assert_eq!(engine.get(&key).unwrap(), Some(expected));
        }
    }

    let _ = std::fs::remove_dir_all(dir);
}
