use criterion::{Criterion, black_box, criterion_group, criterion_main};
use lsm_storage_engine::Engine;
use tempfile::tempdir;

fn bench_engine(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path(), 4 * 1024 * 1024).unwrap();

    c.bench_function("engine_put", |b| {
        let mut i = 0;
        b.iter(|| {
            let key = format!("key_{:010}", i).into_bytes();
            let value = vec![0u8; 100];
            engine.put(black_box(key), black_box(value)).unwrap();
            i += 1;
        })
    });

    c.bench_function("engine_get_hit", |b| {
        // Pre-fill
        for i in 0..1000 {
            let key = format!("key_{:010}", i).into_bytes();
            engine.put(key, vec![0u8; 100]).unwrap();
        }

        let mut i = 0;
        b.iter(|| {
            let key = format!("key_{:010}", i % 1000).into_bytes();
            engine.get(black_box(&key)).unwrap();
            i += 1;
        })
    });

    c.bench_function("engine_get_miss", |b| {
        b.iter(|| {
            let key = b"non_existent_key";
            engine.get(black_box(key)).unwrap();
        })
    });
}

criterion_group!(benches, bench_engine);
criterion_main!(benches);
