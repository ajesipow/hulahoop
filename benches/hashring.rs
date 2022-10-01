use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use hulahoop::HashRing;
use std::num::NonZeroU64;

pub fn criterion_benchmark(c: &mut Criterion) {
    {
        let mut ring: HashRing<&str> = HashRing::new();
        ring.add("127.0.0.1:12345", NonZeroU64::new(1).unwrap());
        ring.add("127.0.0.1:12346", NonZeroU64::new(1).unwrap());
        let mut group = c.benchmark_group("Getting a node for a key from the HashRing");
        for size in ["abc", "1234", "Some very very long text"].iter() {
            group.throughput(Throughput::Bytes(size.as_bytes().len() as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
                b.iter(|| ring.get_by_key(size));
            });
        }
        group.finish();
    }

    {
        let mut ring: HashRing<&str> = HashRing::new();
        let mut group = c.benchmark_group("Adding virtual nodes");
        for size in [1, 10, 100, 1000].iter() {
            group.throughput(Throughput::Bytes(*size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
                b.iter(|| ring.add("127.0.0.1:12346", NonZeroU64::new(size).unwrap()));
            });
        }
        group.finish();
    }

    {
        let mut ring: HashRing<&str> = HashRing::new();
        let mut group = c.benchmark_group("Removing virtual nodes");
        for size in [1, 10, 100, 1000].iter() {
            ring.add("127.0.0.1:12346", NonZeroU64::new(*size).unwrap());
            group.bench_function(BenchmarkId::from_parameter(size), |b| {
                b.iter(|| ring.remove("127.0.0.1:12346"))
            });
        }
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
