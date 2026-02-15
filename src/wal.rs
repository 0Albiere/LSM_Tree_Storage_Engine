use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Represents an entry in the Write-Ahead Log.
#[derive(Debug, PartialEq, Eq)]
pub enum WalEntry {
    /// A record of a put operation.
    Put {
        /// The key being inserted.
        key: Vec<u8>,
        /// The value associated with the key.
        value: Vec<u8>,
    },
    /// A record of a delete operation.
    Delete {
        /// The key being deleted.
        key: Vec<u8>,
    },
}

/// A Write-Ahead Log that provides persistence for the `MemTable`.
///
/// Every write operation is first appended to the WAL before being applied to the in-memory
/// structure, ensuring that data can be recovered after a crash.
pub struct Wal {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl Wal {
    /// Opens the WAL at the specified path. Creates the file if it doesn't exist.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().append(true).create(true).open(&path)?;

        Ok(Self {
            writer: BufWriter::new(file),
            path,
        })
    }

    /// Appends a `WalEntry` to the log and flushes it to disk.
    pub fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
        match entry {
            WalEntry::Put { key, value } => {
                self.writer.write_all(&[0])?; // Type 0 for Put
                self.writer.write_all(&(key.len() as u32).to_le_bytes())?;
                self.writer.write_all(key)?;
                self.writer.write_all(&(value.len() as u32).to_le_bytes())?;
                self.writer.write_all(value)?;
            }
            WalEntry::Delete { key } => {
                self.writer.write_all(&[1])?; // Type 1 for Delete
                self.writer.write_all(&(key.len() as u32).to_le_bytes())?;
                self.writer.write_all(key)?;
            }
        }
        self.writer.flush()?;
        Ok(())
    }

    /// Recovers all entries from the WAL file at the given path.
    pub fn recover(path: impl AsRef<Path>) -> io::Result<Vec<WalEntry>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut type_buf = [0u8; 1];
            if let Err(e) = reader.read_exact(&mut type_buf) {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(e);
            }

            match type_buf[0] {
                0 => {
                    // Put
                    let mut len_buf = [0u8; 4];
                    reader.read_exact(&mut len_buf)?;
                    let key_len = u32::from_le_bytes(len_buf) as usize;
                    let mut key = vec![0u8; key_len];
                    reader.read_exact(&mut key)?;

                    reader.read_exact(&mut len_buf)?;
                    let value_len = u32::from_le_bytes(len_buf) as usize;
                    let mut value = vec![0u8; value_len];
                    reader.read_exact(&mut value)?;

                    entries.push(WalEntry::Put { key, value });
                }
                1 => {
                    // Delete
                    let mut len_buf = [0u8; 4];
                    reader.read_exact(&mut len_buf)?;
                    let key_len = u32::from_le_bytes(len_buf) as usize;
                    let mut key = vec![0u8; key_len];
                    reader.read_exact(&mut key)?;

                    entries.push(WalEntry::Delete { key });
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid WalEntry type",
                    ));
                }
            }
        }

        Ok(entries)
    }

    /// Truncates the WAL, effectively clearing all recorded entries.
    pub fn truncate(&mut self) -> io::Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        Ok(())
    }

    /// Returns the path to the WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_dir(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "lsm_test_{}_{}",
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
    fn test_append_and_recover() {
        let dir = setup_test_dir("append_recover");
        let wal_path = dir.join("test.wal");
        let mut wal = Wal::open(&wal_path).unwrap();

        let entries = vec![
            WalEntry::Put {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            },
            WalEntry::Delete {
                key: b"k2".to_vec(),
            },
        ];

        for entry in &entries {
            wal.append(entry).unwrap();
        }

        let recovered = Wal::recover(&wal_path).unwrap();
        assert_eq!(recovered, entries);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_append_multiple() {
        let dir = setup_test_dir("append_multiple");
        let wal_path = dir.join("test.wal");
        let mut wal = Wal::open(&wal_path).unwrap();

        for i in 0..10 {
            wal.append(&WalEntry::Put {
                key: vec![i as u8],
                value: vec![i as u8; 10],
            })
            .unwrap();
        }

        let file_size = std::fs::metadata(&wal_path).unwrap().len();
        assert_eq!(file_size, 200);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_recover_empty() {
        let dir = setup_test_dir("recover_empty");
        let wal_path = dir.join("empty.wal");
        let recovered = Wal::recover(&wal_path).unwrap();
        assert!(recovered.is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_corrupted_entry() {
        let dir = setup_test_dir("corrupt");
        let wal_path = dir.join("corrupt.wal");
        {
            let mut it = Wal::open(&wal_path).unwrap();
            it.append(&WalEntry::Put {
                key: b"ok".to_vec(),
                value: b"val".to_vec(),
            })
            .unwrap();
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            f.write_all(&[0, 0, 0, 100]).unwrap();
        }

        let recovered = Wal::recover(&wal_path);
        assert!(recovered.is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_truncate() {
        let dir = setup_test_dir("truncate");
        let wal_path = dir.join("test.wal");
        let mut wal = Wal::open(&wal_path).unwrap();
        wal.append(&WalEntry::Put {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        })
        .unwrap();

        wal.truncate().unwrap();
        let recovered = Wal::recover(&wal_path).unwrap();
        assert!(recovered.is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }
}
