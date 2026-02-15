use crate::memtable::{Entry, MemTable};
use crate::sstable::{SSTable, SSTableBuilder};
use crate::wal::{Wal, WalEntry};
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

/// The main LSM-Tree storage engine.
///
/// The `Engine` coordinates the `MemTable`, `Wal`, and `SSTable`s to provide a unified
/// key-value store with persistence and background compaction.
pub struct Engine {
    active_memtable: RwLock<MemTable>,
    wal: RwLock<Wal>,
    sstables: Arc<RwLock<Vec<Arc<SSTable>>>>,
    dir: PathBuf,
    #[allow(dead_code)]
    max_memtable_size: usize,
    compaction_running: Arc<AtomicBool>,
}

impl Engine {
    /// Opens the storage engine in the specified directory.
    ///
    /// Recovers state from the WAL and loads existing SSTables.
    pub fn open(dir: impl AsRef<Path>, max_memtable_size: usize) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }

        let wal_path = dir.join("active.wal");

        let wal_entries = Wal::recover(&wal_path)?;
        let mut memtable = MemTable::new(max_memtable_size);
        for entry in wal_entries {
            match entry {
                WalEntry::Put { key, value } => memtable.put(key, value),
                WalEntry::Delete { key } => memtable.delete(key),
            }
        }

        let wal = Wal::open(&wal_path)?;

        let mut sstables = Vec::new();
        let mut sstable_files: Vec<_> = std::fs::read_dir(&dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("sst"))
            .collect();

        sstable_files.sort_by_key(|e| e.file_name());
        sstable_files.reverse();

        for entry in sstable_files {
            sstables.push(Arc::new(SSTable::open(entry.path())?));
        }

        Ok(Self {
            active_memtable: RwLock::new(memtable),
            wal: RwLock::new(wal),
            sstables: Arc::new(RwLock::new(sstables)),
            dir,
            max_memtable_size,
            compaction_running: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Inserts or updates a key-value pair.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        {
            let mut wal = self.wal.write().unwrap();
            wal.append(&WalEntry::Put {
                key: key.clone(),
                value: value.clone(),
            })?;
        }

        let mut mt = self.active_memtable.write().unwrap();
        mt.put(key, value);

        if mt.is_full() {
            drop(mt);
            self.flush()?;
        }

        Ok(())
    }

    /// Retrieves a value by its key.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        {
            let mt = self.active_memtable.read().unwrap();
            if let Some(entry) = mt.get(key) {
                return match entry {
                    Entry::Value(v) => Ok(Some(v.clone())),
                    Entry::Tombstone => Ok(None),
                };
            }
        }

        let ssts = self.sstables.read().unwrap();
        for sst in ssts.iter() {
            if let Some(val) = sst.get(key)? {
                return Ok(Some(val));
            }
        }

        Ok(None)
    }

    /// Marks a key as deleted.
    pub fn delete(&self, key: Vec<u8>) -> io::Result<()> {
        {
            let mut wal = self.wal.write().unwrap();
            wal.append(&WalEntry::Delete { key: key.clone() })?;
        }

        let mut mt = self.active_memtable.write().unwrap();
        mt.delete(key);

        if mt.is_full() {
            drop(mt);
            self.flush()?;
        }

        Ok(())
    }

    /// Manually triggers a flush of the current MemTable to an SSTable.
    pub fn flush(&self) -> io::Result<()> {
        let mut mt = self.active_memtable.write().unwrap();
        if mt.approximate_size() == 0 {
            return Ok(());
        }

        let sstable_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sst_path = self.dir.join(format!("{:020}.sst", sstable_id));

        let builder = SSTableBuilder::new(&sst_path, 16)?;
        builder.build(&mt)?;

        {
            let mut ssts = self.sstables.write().unwrap();
            ssts.insert(0, Arc::new(SSTable::open(&sst_path)?));
        }

        mt.clear();
        let mut wal = self.wal.write().unwrap();
        wal.truncate()?;

        self.check_compaction();

        Ok(())
    }

    fn check_compaction(&self) {
        if self.compaction_running.load(Ordering::SeqCst) {
            return;
        }

        let sstable_count = {
            let ssts = self.sstables.read().unwrap();
            ssts.len()
        };

        if sstable_count >= 4 {
            if self.compaction_running.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                return;
            }

            let sst_ref = Arc::clone(&self.sstables);
            let dir = self.dir.clone();
            let running_flag = Arc::clone(&self.compaction_running);

            std::thread::spawn(move || {
                let to_compact = {
                    let ssts = sst_ref.read().unwrap();
                    ssts.clone()
                };

                let sstable_id = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let output_path = dir.join(format!("{:020}.compact.sst", sstable_id));

                if let Err(e) = crate::compaction::compact(&to_compact, &output_path) {
                    eprintln!("Compaction failed: {}", e);
                    running_flag.store(false, Ordering::SeqCst);
                    return;
                }

                match SSTable::open(&output_path) {
                    Ok(new_sst) => {
                        let mut ssts = sst_ref.write().unwrap();
                        let compacted_paths: std::collections::HashSet<_> = to_compact.iter().map(|s| s.path().to_path_buf()).collect();
                        ssts.retain(|s| !compacted_paths.contains(s.path()));
                        ssts.push(Arc::new(new_sst));
                    }
                    Err(e) => eprintln!("Failed to open compacted SSTable: {}", e),
                }
                running_flag.store(false, Ordering::SeqCst);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_dir(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("lsm_test_{}_{}", name, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn test_put_get_basic() {
        let dir = setup_test_dir("engine_basic");
        let engine = Engine::open(&dir, 1024).unwrap();
        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        assert_eq!(engine.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_get_nonexistent() {
        let dir = setup_test_dir("engine_nonexistent");
        let engine = Engine::open(&dir, 1024).unwrap();
        assert_eq!(engine.get(b"k1").unwrap(), None);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_update() {
        let dir = setup_test_dir("engine_update");
        let engine = Engine::open(&dir, 1024).unwrap();
        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        engine.put(b"k1".to_vec(), b"v2".to_vec()).unwrap();
        assert_eq!(engine.get(b"k1").unwrap(), Some(b"v2".to_vec()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_delete() {
        let dir = setup_test_dir("engine_delete");
        let engine = Engine::open(&dir, 1024).unwrap();
        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        engine.delete(b"k1".to_vec()).unwrap();
        assert_eq!(engine.get(b"k1").unwrap(), None);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_delete_nonexistent() {
        let dir = setup_test_dir("engine_del_nonexistent");
        let engine = Engine::open(&dir, 1024).unwrap();
        engine.delete(b"k1".to_vec()).unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_flush_trigger() {
        let dir = setup_test_dir("engine_flush");
        let engine = Engine::open(&dir, 10).unwrap();
        engine.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        
        let sstable_count = || {
            std::fs::read_dir(&dir).unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|ext| ext.to_str()) == Some("sst"))
                .count()
        };

        assert_eq!(sstable_count(), 0);
        engine.put(b"key2".to_vec(), b"val2".to_vec()).unwrap();
        assert!(sstable_count() >= 1);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_get_after_flush() {
        let dir = setup_test_dir("engine_get_flush");
        let engine = Engine::open(&dir, 10).unwrap();
        engine.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        engine.put(b"key2".to_vec(), b"val2".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_recovery_from_wal() {
        let dir = setup_test_dir("engine_recovery_wal");
        {
            let engine = Engine::open(&dir, 1024).unwrap();
            engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        } 

        let engine = Engine::open(&dir, 1024).unwrap();
        assert_eq!(engine.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_recovery_with_flush() {
        let dir = setup_test_dir("engine_recovery_flush");
        {
            let engine = Engine::open(&dir, 10).unwrap();
            engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
            engine.put(b"k2".to_vec(), b"v2".to_vec()).unwrap();
        }

        let engine = Engine::open(&dir, 10).unwrap();
        assert_eq!(engine.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_multiple_sstables_read() {
        let dir = setup_test_dir("engine_multiple");
        let engine = Engine::open(&dir, 10).unwrap();
        
        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        engine.put(b"k1_f".to_vec(), b"v".to_vec()).unwrap(); 
        
        engine.put(b"k2".to_vec(), b"v2".to_vec()).unwrap();
        engine.put(b"k2_f".to_vec(), b"v".to_vec()).unwrap();
        
        assert_eq!(engine.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_tombstone_across_sstables() {
        let dir = setup_test_dir("engine_tombstone");
        let engine = Engine::open(&dir, 10).unwrap();
        
        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        engine.put(b"f1".to_vec(), b"v".to_vec()).unwrap();
        
        engine.delete(b"k1".to_vec()).unwrap();
        engine.put(b"f2".to_vec(), b"v".to_vec()).unwrap();
        
        assert_eq!(engine.get(b"k1").unwrap(), None);
        let _ = std::fs::remove_dir_all(dir);
    }
}
