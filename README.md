# Hulahoop ![GitHub Workflow Status](https://img.shields.io/github/workflow/status/ajesipow/hulahoop/Makefile%20CI?style=flat-square) ![docs.rs](https://img.shields.io/docsrs/hulahoop?style=flat-square) ![Crates.io](https://img.shields.io/crates/v/hulahoop?style=flat-square)

**A fast and efficient consistent hashing implementation, with support for virtual nodes.**

---

## Usage

```rust
    use std::num::NonZeroU64;
    use hulahoop::HashRing;

    let mut map: HashRing<&str, _> = HashRing::default();

    // Nodes only need to implement Hash
    // Provide a weight to define the number of virtual nodes
    map.insert("10.0.0.1:1234", 10);
    map.insert("10.0.0.2:1234", 10);

    // Keys also only need to implement Hash
    assert_eq!(map.get("Some key"), Some(&"10.0.0.1:1234"));
    assert_eq!(map.get("Another key"), Some(&"10.0.0.2:1234"));

    map.remove(&"10.0.0.2:1234");

    assert_eq!(map.get("Some key"), Some(&"10.0.0.1:1234"));
    assert_eq!(map.get("Another key"), Some(&"10.0.0.1:1234"));
```

`HashRing` uses `Arc` under the hood to allocate memory only per node and not for every virtual node added via the weight parameter.

The `Hashring` is `Send + Sync`. 

---

## Hashers

Per default, `hulahoop` uses `std::collections::hash_map::DefaultHasher` to hash values.

Custom hashers can be used with the `HashRing::with_hasher()` method:

```rust
    use rustc_hash::FxHasher;
    let mut ring: HashRing<&str, _> = HashRing::with_hasher(BuildHasherDefault::<FxHasher>::default());
```

For convenience, the [faster](https://nnethercote.github.io/perf-book/hashing.html) hasher [FxHasher](https://docs.rs/rustc-hash/1.1.0/rustc_hash/struct.FxHasher.html) can be used by activating the `fxhash` feature of this crate. 


---

## Benchmarks

|  | DefaultHasher | FxHasher (feature=fxhash) |
|---|--------------:|--------------------------:|
| Get (key length = 10) |          13ns |                       8ns |
| Get (key length = 100) |          31ns |                      12ns |
| Get (key length = 1000) |         305ns |                     137ns |
| Add (weight = 1) |         290ns |                     210ns |
| Add (weight = 10) |         1.4us |                     1.0us |
| Add (weight = 100) |        17.0us |                    14.3us |

---

## License

This project is licensed optionally under either:
* Apache License, Version 2.0, (LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0)
* MIT license (LICENSE-MIT or https://opensource.org/licenses/MIT)
