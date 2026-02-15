use lsm_storage_engine::{Engine, Entry};
use std::path::PathBuf;
use std::time::SystemTime;

fn setup_test_dir(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("lsm_crash_{}_{}", name, SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn test_crash_recovery_from_wal() {
    let dir = setup_test_dir("wal_crash");
    
    // 1. Initial writes
    {
        let engine = Engine::open(&dir, 1024 * 1024).unwrap();
        engine.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        engine.put(b"key2".to_vec(), b"val2".to_vec()).unwrap();
        // Engine is closed without flush (simulated crash)
    }

    // 2. Re-open engine and verify data is recovered from WAL
    {
        let engine = Engine::open(&dir, 1024 * 1024).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(engine.get(b"key2").unwrap(), Some(b"val2".to_vec()));
    }

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_crash_during_flush_simulation() {
    let dir = setup_test_dir("flush_crash");
    
    // 1. Write data and manually trigger some state
    {
        let engine = Engine::open(&dir, 1024 * 1024).unwrap();
        engine.put(b"persistent_key".to_vec(), b"old_val".to_vec()).unwrap();
        engine.flush().unwrap(); // SSTable 1 created
        
        engine.put(b"volatile_key".to_vec(), b"new_val".to_vec()).unwrap();
        // Simulated crash here (before flush of volatile_key)
    }

    // 2. Re-open and verify both persistent (SSTable) and volatile (WAL) data are there
    {
        let engine = Engine::open(&dir, 1024 * 1024).unwrap();
        assert_eq!(engine.get(b"persistent_key").unwrap(), Some(b"old_val".to_vec()));
        assert_eq!(engine.get(b"volatile_key").unwrap(), Some(b"new_val".to_vec()));
    }

    let _ = std::fs::remove_dir_all(dir);
}
