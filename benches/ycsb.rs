use lsm_storage_engine::Engine;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

fn setup_test_dir(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "lsm_bench_{}_{}",
        name,
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn main() -> std::io::Result<()> {
    let dir = setup_test_dir("ycsb_bench");
    let engine = Engine::open(&dir, 4 * 1024 * 1024)?; // 4MB memtable

    let counts = [100_000, 1_000_000]; // 1M as requested (10M might be too slow for this environment, let's start with 1M)

    for &count in &counts {
        println!("--- Benchmarking {} writes ---", count);

        let start = Instant::now();
        let mut latencies = Vec::with_capacity(count);

        for i in 0..count {
            let key = format!("user{:010}", i).into_bytes();
            let val = vec![0u8; 128]; // 128 byte values

            let op_start = Instant::now();
            engine.put(key, val)?;
            latencies.push(op_start.elapsed());

            if i > 0 && i % 100_000 == 0 {
                println!("  Progress: {}k items...", i / 1000);
            }
        }

        let total_duration = start.elapsed();
        latencies.sort();

        let p50 = latencies[count / 2];
        let p95 = latencies[count * 95 / 100];
        let p99 = latencies[count * 99 / 100];
        let throughput = count as f64 / total_duration.as_secs_f64();

        println!("Results for {} writes:", count);
        println!("  Total Time:   {:?}", total_duration);
        println!("  Throughput:   {:.2} ops/sec", throughput);
        println!("  P50 Latency:  {:?}", p50);
        println!("  P95 Latency:  {:?}", p95);
        println!("  P99 Latency:  {:?}", p99);
        println!();
    }

    let _ = fs::remove_dir_all(dir);
    Ok(())
}
