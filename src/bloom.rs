use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A probabilistic data structure for membership testing.
///
/// The `BloomFilter` is used to quickly check if a key *might* be present in an SSTable
/// or if it's definitely not. This avoids unnecessary disk access for lookups.
pub struct BloomFilter {
    bits: Vec<u8>,
    num_hashes: usize,
    num_bits: usize,
}

impl BloomFilter {
    /// Creates a new `BloomFilter` optimized for the expected number of items and false positive rate.
    pub fn new(num_items: usize, false_positive_rate: f64) -> Self {
        // Optimal size calculations
        // m = -(n * ln(p)) / (ln(2)^2)
        // k = (m/n) * ln(2)
        let n = num_items as f64;
        let p = false_positive_rate;

        let m = (-(n * p.ln()) / (2.0f64.ln().powi(2))).ceil() as usize;
        let k = ((m as f64 / n) * 2.0f64.ln()).ceil() as usize;

        let num_bytes = m.div_ceil(8);
        Self {
            bits: vec![0u8; num_bytes],
            num_hashes: k,
            num_bits: num_bytes * 8,
        }
    }

    /// Adds a key to the `BloomFilter`.
    pub fn add(&mut self, key: &[u8]) {
        for i in 0..self.num_hashes {
            let h = self.hash(key, i);
            let bit_pos = h % self.num_bits;
            self.bits[bit_pos / 8] |= 1 << (bit_pos % 8);
        }
    }

    /// Checks if a key might be in the `BloomFilter`.
    pub fn contains(&self, key: &[u8]) -> bool {
        if self.num_bits == 0 {
            return false;
        }
        for i in 0..self.num_hashes {
            let h = self.hash(key, i);
            let bit_pos = h % self.num_bits;
            if (self.bits[bit_pos / 8] & (1 << (bit_pos % 8))) == 0 {
                return false;
            }
        }
        true
    }

    fn hash(&self, key: &[u8], i: usize) -> usize {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        i.hash(&mut s);
        s.finish() as usize
    }

    /// Serializes the `BloomFilter` into a byte vector.
    pub fn serialize(&self) -> Vec<u8> {
        let mut res = Vec::new();
        res.extend_from_slice(&(self.num_hashes as u32).to_le_bytes());
        res.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        res.extend_from_slice(&self.bits);
        res
    }

    /// Deserializes a `BloomFilter` from a byte slice.
    pub fn deserialize(data: &[u8]) -> Self {
        let num_hashes = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let num_bits = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        let bits = data[8..].to_vec();
        Self {
            bits,
            num_hashes,
            num_bits,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_add_contains() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.add(b"key1");
        bf.add(b"key2");
        assert!(bf.contains(b"key1"));
        assert!(bf.contains(b"key2"));
    }

    #[test]
    fn test_bloom_false_negative() {
        let mut bf = BloomFilter::new(1000, 0.01);
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            bf.add(&key);
            assert!(bf.contains(&key), "False negative for key{}", i);
        }
    }

    #[test]
    fn test_bloom_false_positive_rate() {
        let mut bf = BloomFilter::new(1000, 0.01);
        for i in 0..1000 {
            bf.add(format!("key{}", i).as_bytes());
        }

        let mut fps = 0;
        for i in 1000..2000 {
            if bf.contains(format!("key{}", i).as_bytes()) {
                fps += 1;
            }
        }
        let rate = fps as f64 / 1000.0;
        assert!(rate <= 0.05, "False positive rate too high: {}", rate); // Alvo 1%, mas permitimos 5% para variância em testes pequenos
    }

    #[test]
    fn test_bloom_serialization() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.add(b"bloom");
        let data = bf.serialize();
        let bf2 = BloomFilter::deserialize(&data);
        assert!(bf2.contains(b"bloom"));
        assert!(!bf2.contains(b"not-bloom"));
    }

    #[test]
    fn test_bloom_empty() {
        let bf = BloomFilter::new(100, 0.01);
        assert!(!bf.contains(b"anything"));
    }
}
