use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use hulahoop::HashRing;
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;

pub fn criterion_benchmark(c: &mut Criterion) {
    {
        let mut ring: HashRing<&str, _> = HashRing::new();
        ring.insert("10.0.0.1:12345", 1);
        ring.insert("10.0.0.2:12345", 1);
        let mut group =
            c.benchmark_group("Getting a node for a key from the HashRing with DefaultHasher");
        for size in [1, 10, 100, 1000, 10000].iter() {
            let key = "a".repeat(*size);
            group.bench_with_input(BenchmarkId::from_parameter(size), &key, |b, key| {
                b.iter(|| ring.get(key));
            });
        }
        group.finish();
    }

    {
        let mut ring: HashRing<&str, _> =
            HashRing::with_hasher(BuildHasherDefault::<FxHasher>::default());
        ring.insert("10.0.0.1:12345", 1);
        ring.insert("10.0.0.2:12345", 1);
        let mut group =
            c.benchmark_group("Getting a node for a key from the HashRing with FxHasher");
        for size in [1, 10, 100, 1000, 10000].iter() {
            let key = "a".repeat(*size);
            group.bench_with_input(BenchmarkId::from_parameter(size), &key, |b, key| {
                b.iter(|| ring.get(key));
            });
        }
        group.finish();
    }

    {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let mut group = c.benchmark_group("Get node for key len 100 DefaultHasher w/ n nodes");
        for size in [1, 10, 100, 1000, 10000].iter() {
            ring.insert("10.0.0.1:12345", *size);
            let key = "a".repeat(100);
            group.bench_with_input(BenchmarkId::from_parameter(size), &key, |b, key| {
                b.iter(|| ring.get(key));
            });
            ring.remove(&"10.0.0.1:12345");
        }
        group.finish();
    }

    {
        let mut ring: HashRing<&str, _> =
            HashRing::with_hasher(BuildHasherDefault::<FxHasher>::default());
        let mut group = c.benchmark_group("Get node for key len 100 FxHahser w/ n nodes");
        for size in [1, 10, 100, 1000, 10000].iter() {
            ring.insert("10.0.0.1:12345", *size);
            let key = "a".repeat(100);
            group.bench_with_input(BenchmarkId::from_parameter(size), &key, |b, key| {
                b.iter(|| ring.get(key));
            });
            ring.remove(&"10.0.0.1:12345");
        }
        group.finish();
    }

    {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let mut group = c.benchmark_group("Inserting virtual nodes");
        for size in [1, 10, 100, 1000].iter() {
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
                b.iter(|| ring.insert("10.0.0.1:12345", size));
            });
        }
        group.finish();
    }

    {
        let mut ring: HashRing<&str, _> =
            HashRing::with_hasher(BuildHasherDefault::<FxHasher>::default());
        let mut group = c.benchmark_group("Inserting virtual nodes with FxHasher");
        for size in [1, 10, 100, 1000].iter() {
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
                b.iter(|| ring.insert("10.0.0.1:12345", size));
            });
        }
        group.finish();
    }

    {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let mut group = c.benchmark_group("Removing virtual nodes");
        for size in [1, 10, 100, 1000].iter() {
            ring.insert("10.0.0.1:12345", *size);
            group.bench_function(BenchmarkId::from_parameter(size), |b| {
                b.iter(|| ring.remove(&"10.0.0.1:12345"))
            });
        }
        group.finish();
    }

    {
        let mut ring: HashRing<&str, _> =
            HashRing::with_hasher(BuildHasherDefault::<FxHasher>::default());
        let mut group = c.benchmark_group("Removing virtual nodes with FxHasher");
        for size in [1, 10, 100, 1000].iter() {
            ring.insert("10.0.0.1:12345", *size);
            group.bench_function(BenchmarkId::from_parameter(size), |b| {
                b.iter(|| ring.remove(&"10.0.0.1:12345"))
            });
        }
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
