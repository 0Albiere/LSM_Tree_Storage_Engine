use lsm_storage_engine::Engine;
use std::path::PathBuf;
use std::time::Duration;

fn setup_test_dir(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("lsm_test_int_{}_{}", name, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn test_engine_operations_sequence() {
    let dir = setup_test_dir("sequence");
    let engine = Engine::open(&dir, 1024).unwrap();

    // 1. Put some keys
    engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
    engine.put(b"k2".to_vec(), b"v2".to_vec()).unwrap();
    
    // 2. Check existence
    assert_eq!(engine.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(engine.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    
    // 3. Update a key
    engine.put(b"k1".to_vec(), b"v1_new".to_vec()).unwrap();
    assert_eq!(engine.get(b"k1").unwrap(), Some(b"v1_new".to_vec()));
    
    // 4. Delete a key
    engine.delete(b"k2".to_vec()).unwrap();
    assert_eq!(engine.get(b"k2").unwrap(), None);
    
    // 5. Force flush
    engine.flush().unwrap();
    assert_eq!(engine.get(b"k1").unwrap(), Some(b"v1_new".to_vec()));
    assert_eq!(engine.get(b"k2").unwrap(), None);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_engine_with_compaction_integration() {
    let dir = setup_test_dir("comp_int");
    // Use very small memtable to trigger many flushes
    let engine = Engine::open(&dir, 20).unwrap();

    for i in 0..100 {
        let key = format!("k{:03}", i).into_bytes();
        let val = vec![i as u8; 20];
        engine.put(key, val).unwrap();
    }

    // Give some time for background compaction
    std::thread::sleep(Duration::from_millis(500));

    // Verify all keys
    for i in 0..100 {
        let key = format!("k{:03}", i).into_bytes();
        let expected = vec![i as u8; 20];
        assert_eq!(engine.get(&key).unwrap(), Some(expected), "Failed at key {}", i);
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_recovery_full_scenario() {
    let dir = setup_test_dir("recovery");
    {
        let engine = Engine::open(&dir, 50).unwrap();
        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        engine.put(b"k2".to_vec(), b"v2".to_vec()).unwrap();
        engine.delete(b"k3".to_vec()).unwrap();
    }

    // Reopen and check
    let engine = Engine::open(&dir, 50).unwrap();
    assert_eq!(engine.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(engine.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(engine.get(b"k3").unwrap(), None);
    let _ = std::fs::remove_dir_all(dir);
}
