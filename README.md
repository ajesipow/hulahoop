# Hulahoop ![GitHub Workflow Status](https://img.shields.io/github/workflow/status/ajesipow/hulahoop/Makefile%20CI?style=flat-square) ![docs.rs](https://img.shields.io/docsrs/hulahoop?style=flat-square) ![Crates.io](https://img.shields.io/crates/v/hulahoop?style=flat-square)

**A fast and efficient consistent hashing implementation, with support for virtual nodes.**

---

## Example

```rust
    use std::num::NonZeroU64;
    use hulahoop::HashRing;

    let mut hashring: HashRing<&str, _> = HashRing::default();

    // Nodes only need to implement Hash
    // Provide a weight to define the number of virtual nodes
    hashring.add("10.0.0.1:1234", NonZeroU64::new(10).unwrap());
    hashring.add("10.0.0.2:1234", NonZeroU64::new(10).unwrap());

    // Keys also only need to implement Hash
    assert_eq!(hashring.get("Some key"), Some(&"10.0.0.1:1234"));
    assert_eq!(hashring.get("Another key"), Some(&"10.0.0.2:1234"));

    hashring.remove("10.0.0.2:1234");

    assert_eq!(hashring.get("Some key"), Some(&"10.0.0.1:1234"));
    assert_eq!(hashring.get("Another key"), Some(&"10.0.0.1:1234"));
```
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

|                          | DefaultHasher | FxHasher |
|--------------------------|---------------|----------|
| Get (key length = 10)    |          10ns | 5ns      |
| Get (key length = 100)   |          11ns | 5ns      |
| Get (key length = 1000)  |          30ns | 11ns     |
| Get (key length = 10000) |         300ns | 140ns    |
| Add (weight = 1)         |          65ns | 65ns     |
| Add (weight = 10)        |         260ns | 260ns    |
| Add (weight = 100)       |         2.3us | 2.3us    |

---

## License

This project is licensed optionally under either:
* Apache License, Version 2.0, (LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0)
* MIT license (LICENSE-MIT or https://opensource.org/licenses/MIT)
