use crate::memtable::Entry;
use crate::sstable::{RecordIterator, SSTable, SSTableBuilder};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io;
use std::path::Path;
use std::sync::Arc;

struct IterItem {
    key: Vec<u8>,
    entry: Entry,
    sstable_index: usize,
    iterator: RecordIterator,
}

impl PartialEq for IterItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.sstable_index == other.sstable_index
    }
}

impl Eq for IterItem {}

impl PartialOrd for IterItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IterItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // We want a min-heap on key.
        // For the same key, we want to prioritize the newest SSTable (lowest index in the slice we received).
        match other.key.cmp(&self.key) {
            Ordering::Equal => self.sstable_index.cmp(&other.sstable_index).reverse(),
            ord => ord,
        }
    }
}

/// Compacts a list of SSTables into a single, optimized SSTable.
///
/// This function uses a k-way merge algorithm to combine multiple SSTables,
/// keeping only the latest version of each key and discarding overwritten records.
pub fn compact(sstables: &[Arc<SSTable>], output_path: &Path) -> io::Result<()> {
    if sstables.is_empty() {
        return Ok(());
    }

    let mut heap = BinaryHeap::new();

    for (i, sst) in sstables.iter().enumerate() {
        let mut iter = sst.iter()?;
        if let Some(result) = iter.next() {
            let (key, entry) = result?;
            heap.push(IterItem {
                key,
                entry,
                sstable_index: i,
                iterator: iter,
            });
        }
    }

    let mut builder = SSTableBuilder::new(output_path, 16)?;
    let mut last_key: Option<Vec<u8>> = None;

    while let Some(mut current) = heap.pop() {
        // If this key is the same as the last one, it's an older version, so skip it
        if let Some(ref lk) = last_key
            && lk == &current.key
        {
            // Advance this iterator and push back if not empty
            if let Some(result) = current.iterator.next() {
                let (next_key, next_entry) = result?;
                current.key = next_key;
                current.entry = next_entry;
                heap.push(current);
            }
            continue;
        }

        // This is the newest version of this key
        last_key = Some(current.key.clone());

        // Write to new SSTable
        builder.add_record(&current.key, &current.entry)?;

        // Advance iterator and push back
        if let Some(result) = current.iterator.next() {
            let (next_key, next_entry) = result?;
            current.key = next_key;
            current.entry = next_entry;
            heap.push(current);
        }
    }

    builder.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::MemTable;
    use std::path::PathBuf;

    fn setup_test_dir(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "lsm_test_compact_{}_{}",
            name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn test_compact_merge() {
        let dir = setup_test_dir("merge");

        let mut mt1 = MemTable::new(1024);
        mt1.put(b"k1".to_vec(), b"v1_old".to_vec());
        mt1.put(b"k2".to_vec(), b"v2".to_vec());
        let sst1_path = dir.join("sst1.sst");
        SSTableBuilder::new(&sst1_path, 1)
            .unwrap()
            .build(&mt1)
            .unwrap();

        let mut mt2 = MemTable::new(1024);
        mt2.put(b"k1".to_vec(), b"v1_new".to_vec());
        let sst2_path = dir.join("sst2.sst");
        SSTableBuilder::new(&sst2_path, 1)
            .unwrap()
            .build(&mt2)
            .unwrap();

        let sst1 = Arc::new(SSTable::open(&sst1_path).unwrap());
        let sst2 = Arc::new(SSTable::open(&sst2_path).unwrap());

        let output_path = dir.join("compact.sst");
        compact(&[sst2, sst1], &output_path).unwrap();

        let meta = std::fs::metadata(&output_path).unwrap();
        println!("Compacted file size: {}", meta.len());
        assert!(meta.len() >= 32);

        let compacted = SSTable::open(&output_path).unwrap();
        assert_eq!(compacted.get(b"k1").unwrap(), Some(b"v1_new".to_vec()));
        assert_eq!(compacted.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_compact_remove_tombstone() {
        let dir = setup_test_dir("tombstone");

        let mut mt1 = MemTable::new(1024);
        mt1.put(b"k1".to_vec(), b"v1".to_vec());
        let sst1_path = dir.join("sst1.sst");
        SSTableBuilder::new(&sst1_path, 1)
            .unwrap()
            .build(&mt1)
            .unwrap();

        let mut mt2 = MemTable::new(1024);
        mt2.delete(b"k1".to_vec());
        let sst2_path = dir.join("sst2.sst");
        SSTableBuilder::new(&sst2_path, 1)
            .unwrap()
            .build(&mt2)
            .unwrap();

        let sst1 = Arc::new(SSTable::open(&sst1_path).unwrap());
        let sst2 = Arc::new(SSTable::open(&sst2_path).unwrap());

        let output_path = dir.join("compact.sst");
        compact(&[sst2, sst1], &output_path).unwrap();

        let compacted = SSTable::open(&output_path).unwrap();
        assert_eq!(compacted.get(b"k1").unwrap(), None);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_compact_no_duplicates() {
        let dir = setup_test_dir("duplicates");

        let mut mt1 = MemTable::new(1024);
        mt1.put(b"a".to_vec(), b"1".to_vec());
        let sst1_path = dir.join("sst1.sst");
        SSTableBuilder::new(&sst1_path, 1)
            .unwrap()
            .build(&mt1)
            .unwrap();

        let mut mt2 = MemTable::new(1024);
        mt2.put(b"b".to_vec(), b"2".to_vec());
        let sst2_path = dir.join("sst2.sst");
        SSTableBuilder::new(&sst2_path, 1)
            .unwrap()
            .build(&mt2)
            .unwrap();

        let output_path = dir.join("compact.sst");
        compact(
            &[
                Arc::new(SSTable::open(&sst1_path).unwrap()),
                Arc::new(SSTable::open(&sst2_path).unwrap()),
            ],
            &output_path,
        )
        .unwrap();

        let compacted = SSTable::open(&output_path).unwrap();
        let mut count = 0;
        let mut iter = compacted.iter().unwrap();
        while let Some(_) = iter.next() {
            count += 1;
        }
        assert_eq!(count, 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_compact_empty_sstables() {
        let dir = setup_test_dir("empty");
        let output_path = dir.join("compact.sst");
        assert!(compact(&[], &output_path).is_ok());
        let _ = std::fs::remove_dir_all(dir);
    }
}
