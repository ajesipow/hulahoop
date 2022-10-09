use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use hulahoop::HashRing;
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;
use std::num::NonZeroU64;

pub fn criterion_benchmark(c: &mut Criterion) {
    {
        let mut ring: HashRing<&str, _> = HashRing::new();
        ring.add("10.0.0.1:12345", NonZeroU64::new(1).unwrap());
        ring.add("10.0.0.2:12345", NonZeroU64::new(1).unwrap());
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
        ring.add("10.0.0.1:12345", NonZeroU64::new(1).unwrap());
        ring.add("10.0.0.2:12345", NonZeroU64::new(1).unwrap());
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
        let mut group = c.benchmark_group("Adding virtual nodes");
        for size in [1, 10, 100, 1000].iter() {
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
                b.iter(|| ring.add("10.0.0.1:12345", NonZeroU64::new(size).unwrap()));
            });
        }
        group.finish();
    }

    {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let mut group = c.benchmark_group("Removing virtual nodes");
        for size in [1, 10, 100, 1000].iter() {
            ring.add("10.0.0.1:12345", NonZeroU64::new(*size).unwrap());
            group.bench_function(BenchmarkId::from_parameter(size), |b| {
                b.iter(|| ring.remove("10.0.0.1:12345"))
            });
        }
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
