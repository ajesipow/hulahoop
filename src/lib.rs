//! # Hulahoop
//!
//! `hulahoop` provides a consistent hashing algorithm with support for virtual nodes.

#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![cfg_attr(test, deny(rust_2018_idioms))]
#![cfg_attr(all(test, feature = "full"), deny(unreachable_pub))]
#![cfg_attr(all(test, feature = "full"), deny(warnings))]
#![cfg_attr(all(test, feature = "nightly"), feature(test))]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "fxhash")]
use rustc_hash::FxHasher;
use std::borrow::Borrow;
#[cfg(not(feature = "fxhash"))]
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::BuildHasherDefault;
use std::hash::{BuildHasher, Hash, Hasher};
use std::num::NonZeroU64;
use std::sync::Arc;

#[derive(Debug)]
struct MasterNode<N> {
    node: N,
    weight: NonZeroU64,
}

/// A [hash ring] for consistent hashing.
///
///
/// # Examples
///
/// ```
/// use std::num::NonZeroU64;
/// use hulahoop::HashRing;
/// let mut map: HashRing<&str, _> = HashRing::new();
/// map.add("127.0.0.1:1234", 1);
/// assert_eq!(map.get("Some key"), Some(&"127.0.0.1:1234"));
/// ```
#[derive(Debug)]
pub struct HashRing<N, B> {
    virtual_nodes: BTreeMap<u64, Arc<MasterNode<N>>>,
    hash_builder: B,
}

#[cfg(not(feature = "fxhash"))]
impl<N> Default for HashRing<N, BuildHasherDefault<DefaultHasher>> {
    fn default() -> Self {
        Self {
            virtual_nodes: Default::default(),
            hash_builder: BuildHasherDefault::default(),
        }
    }
}

#[cfg(not(feature = "fxhash"))]
impl<N> HashRing<N, BuildHasherDefault<DefaultHasher>> {
    /// Creates a new `HashRing` with the default hasher.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroU64;
    /// use hulahoop::HashRing;
    /// let mut map: HashRing<&str, _> = HashRing::new();
    /// map.add("127.0.0.1:1234", 1);
    /// assert_eq!(map.get("Some key"), Some(&"127.0.0.1:1234"));
    /// ```
    pub fn new() -> Self {
        Self {
            virtual_nodes: BTreeMap::new(),
            hash_builder: BuildHasherDefault::default(),
        }
    }
}

#[cfg(feature = "fxhash")]
impl<N> Default for HashRing<N, BuildHasherDefault<FxHasher>> {
    fn default() -> Self {
        Self {
            virtual_nodes: Default::default(),
            hash_builder: BuildHasherDefault::default(),
        }
    }
}

#[cfg(feature = "fxhash")]
impl<N> HashRing<N, BuildHasherDefault<FxHasher>> {
    /// Creates a new `HashRing` with the default hasher.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroU64;
    /// use hulahoop::HashRing;
    /// let mut map: HashRing<&str, _> = HashRing::new();
    /// map.add("127.0.0.1:1234", 1);
    /// assert_eq!(map.get("Some key"), Some(&"127.0.0.1:1234"));
    /// ```
    pub fn new() -> Self {
        Self {
            virtual_nodes: BTreeMap::new(),
            hash_builder: BuildHasherDefault::default(),
        }
    }
}

impl<N, B> HashRing<N, B>
where
    N: Hash,
    B: BuildHasher,
{
    /// Creates an empty `HashRing` which will use the given `hash_builder` to hash nodes and keys.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::hash::BuildHasherDefault;
    /// use std::num::NonZeroU64;
    /// use rustc_hash::FxHasher;
    /// use hulahoop::HashRing;
    /// let mut map: HashRing<&str, BuildHasherDefault<FxHasher>> = HashRing::with_hasher(BuildHasherDefault::<FxHasher>::default());
    /// map.add("127.0.0.1:1234", 1);
    /// assert_eq!(map.get("Some key"), Some(&"127.0.0.1:1234"));
    /// ```
    pub fn with_hasher(hash_builder: B) -> Self {
        Self {
            virtual_nodes: BTreeMap::new(),
            hash_builder,
        }
    }

    /// Adds a node to the `HashRing`.
    ///
    /// A positive `weight`, representing the number of virtual nodes for the given `node`, must be provided.
    ///
    /// There can be hash collisions resulting in fewer than `weight` virtual nodes added.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroU64;
    /// use hulahoop::HashRing;
    /// let mut map: HashRing<&str, _> = HashRing::default();
    /// map.add("127.0.0.1:1234", 1);
    /// ```
    pub fn add(&mut self, node: N, weight: u64) -> Option<N> {
        if weight == 0 {
            return None;
        }
        let weight = NonZeroU64::new(weight).unwrap();
        let virtual_node_hashes = self.compute_virtual_node_hashes(&node, weight);
        let master_node = Arc::new(MasterNode { node, weight });
        let mut colliding_node = None;
        for virtual_node_hash in virtual_node_hashes.into_iter() {
            if let Some(existing_node) = self
                .virtual_nodes
                .insert(virtual_node_hash, master_node.clone())
            {
                if let Ok(node) = Arc::try_unwrap(existing_node) {
                    colliding_node = Some(node.node);
                }
            }
        }
        colliding_node
    }

    /// Returns a reference to the node with a hash closest to the hash of the key.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroU64;
    /// use hulahoop::HashRing;
    /// let mut map: HashRing<&str, _> = HashRing::default();
    /// map.add("127.0.0.1:1234",1);
    /// assert_eq!(map.get("Some key"), Some(&"127.0.0.1:1234"));
    /// assert_eq!(map.get(12345), Some(&"127.0.0.1:1234"));
    /// ```
    #[inline]
    pub fn get<K>(&self, key: K) -> Option<&N>
    where
        K: Hash,
    {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        let key_hash = hasher.finish();
        match self.virtual_nodes.range(key_hash..).next() {
            Some((_, virtual_node)) => Some(virtual_node.node.borrow()),
            None => {
                // We couldn't find any node greater than the key hash,
                // so let's loop around and take the first one in the HashRing if available.
                self.virtual_nodes
                    .iter()
                    .next()
                    .map(|(_, virtual_node)| virtual_node.node.borrow())
            }
        }
    }

    fn get_master_node_by_hash(&self, hash: &u64) -> Option<&MasterNode<N>> {
        self.virtual_nodes.get(hash).map(|node| node.as_ref())
    }

    fn compute_virtual_node_hashes(&self, node: &N, weight: NonZeroU64) -> Vec<u64> {
        (0..weight.get())
            .into_iter()
            .map(|virtual_node_identifier| {
                let mut hasher = self.hash_builder.build_hasher();
                node.hash(&mut hasher);
                hasher.write_u64(virtual_node_identifier);
                // It could be that we have a collision here and therefore fewer virtual nodes
                // TODO what's the distribution here?
                hasher.finish()
            })
            .collect()
    }

    /// Removes a node from the `HashRing`, returning the number of virtual nodes (weight) of the removed node.
    ///
    /// # Examples
    ///
    /// ```
    /// use hulahoop::HashRing;
    /// let mut map: HashRing<&str, _> = HashRing::default();
    /// map.add("127.0.0.1:1234", 10);
    /// assert_eq!(map.remove("127.0.0.1:1234"), 10);
    /// assert_eq!(map.remove("127.0.0.1:1234"), 0);
    /// ```
    pub fn remove(&mut self, node: N) -> u64 {
        // At least one node should exist
        let virtual_node_hashes =
            self.compute_virtual_node_hashes(&node, NonZeroU64::new(1).unwrap());
        let one_node = virtual_node_hashes
            .first()
            .and_then(|hash| self.get_master_node_by_hash(hash));
        match one_node {
            Some(master_node) => {
                let mut removed_nodes = 0;
                for virtual_node_hash in self
                    .compute_virtual_node_hashes(&master_node.node, master_node.weight)
                    .iter()
                {
                    if self.virtual_nodes.remove(virtual_node_hash).is_some() {
                        removed_nodes += 1;
                    };
                }
                removed_nodes
            }
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adding_a_node_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.add(node, 1);

        assert_eq!(ring.virtual_nodes.len(), 1);
    }

    #[test]
    fn adding_a_node_with_many_virtual_nodes_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.add(node, 100);

        assert_eq!(ring.virtual_nodes.len(), 100);
    }

    #[test]
    fn adding_multiple_nodes_with_many_virtual_nodes_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node_1 = "10.0.0.1:12345";
        let node_2 = "20.0.0.1:12345";
        ring.add(node_1, 100);
        ring.add(node_2, 500);

        assert_eq!(ring.virtual_nodes.len(), 600);
    }

    #[test]
    fn removing_a_node_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.add(node, 1);
        assert_eq!(ring.virtual_nodes.len(), 1);

        ring.remove(node);
        assert_eq!(ring.virtual_nodes.len(), 0);
    }

    #[test]
    fn removing_a_node_with_many_virtual_nodes_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.add(node, 100);
        assert_eq!(ring.virtual_nodes.len(), 100);

        let nodes_removed = ring.remove(node);
        assert_eq!(nodes_removed, 100);
        assert_eq!(ring.virtual_nodes.len(), 0);
    }

    #[test]
    fn removing_multiple_nodes_with_many_virtual_nodes_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node_1 = "10.0.0.1:12345";
        let node_2 = "20.0.0.1:12345";
        ring.add(node_1, 100);
        ring.add(node_2, 500);
        assert_eq!(ring.virtual_nodes.len(), 600);

        let nodes_removed = ring.remove(node_1);
        assert_eq!(nodes_removed, 100);
        assert_eq!(ring.virtual_nodes.len(), 500);

        let nodes_removed = ring.remove(node_2);
        assert_eq!(nodes_removed, 500);
        assert_eq!(ring.virtual_nodes.len(), 0);
    }

    #[test]
    fn adding_one_node_and_getting_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.add(node, 1);

        let node_for_val_a = ring.get("abc");
        let node_for_val_b = ring.get(12345);

        assert_eq!(node_for_val_a, Some(&node));
        assert_eq!(node_for_val_b, Some(&node));
    }

    #[test]
    fn adding_multiple_nodes_and_getting_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node_1 = "10.0.0.1:12345";
        let node_2 = "20.0.0.1:12345";
        let node_3 = "30.0.0.1:12345";
        ring.add(node_1, 1);
        ring.add(node_2, 1);
        ring.add(node_3, 1);

        let node_for_val_a = ring.get("hula");
        let node_for_val_b = ring.get(12345);
        let node_for_val_c = ring.get(54321);
        let node_for_val_d = ring.get(b"12345");

        assert_eq!(node_for_val_a, Some(&node_3));
        assert_eq!(node_for_val_b, Some(&node_2));
        assert_eq!(node_for_val_c, Some(&node_3));
        assert_eq!(node_for_val_d, Some(&node_1));
    }

    #[test]
    fn adding_multiple_nodes_getting_and_removing_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node_1 = "10.0.0.1:12345";
        let node_2 = "20.0.0.1:12345";
        let node_3 = "30.0.0.1:12345";
        ring.add(node_1, 1);
        ring.add(node_2, 1);
        ring.add(node_3, 1);

        let key_1 = "hula";
        let key_2 = 12345;
        let key_3 = 54321;
        let key_4 = b"12345";

        {
            let node_for_key_1 = ring.get(key_1);
            let node_for_val_2 = ring.get(key_2);
            let node_for_key_3 = ring.get(key_3);
            let node_for_key_4 = ring.get(key_4);

            assert_eq!(node_for_key_1, Some(&node_3));
            assert_eq!(node_for_val_2, Some(&node_2));
            assert_eq!(node_for_key_3, Some(&node_3));
            assert_eq!(node_for_key_4, Some(&node_1));
        }

        ring.remove(node_1);

        {
            let node_for_key_1 = ring.get(key_1);
            let node_for_key_2 = ring.get(key_2);
            let node_for_key_3 = ring.get(key_3);
            let node_for_key_4 = ring.get(key_4);

            assert_eq!(node_for_key_1, Some(&node_3));
            assert_eq!(node_for_key_2, Some(&node_2));
            assert_eq!(node_for_key_3, Some(&node_3));
            // Only this value got remapped
            assert_eq!(node_for_key_4, Some(&node_2));
        }
    }

    #[test]
    fn creating_a_hashring_with_custom_hasher_and_adding_and_getting_works() {
        use rustc_hash::FxHasher;
        let mut ring: HashRing<&str, _> =
            HashRing::with_hasher(BuildHasherDefault::<FxHasher>::default());
        let node = "10.0.0.1:12345";
        ring.add(node, 1);

        let node_for_val_a = ring.get("abc");
        let node_for_val_b = ring.get(12345);

        assert_eq!(node_for_val_a, Some(&node));
        assert_eq!(node_for_val_b, Some(&node));
    }

    #[derive(Default, Debug)]
    struct CollisionHasher;
    impl Hasher for CollisionHasher {
        fn finish(&self) -> u64 {
            // To cause hash collisions
            1
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    #[test]
    fn virtual_node_collisions_not_an_issue() {
        let mut ring: HashRing<&str, _> =
            HashRing::with_hasher(BuildHasherDefault::<CollisionHasher>::default());
        let node = "10.0.0.1:12345";
        let node_2 = "10.0.0.2:12345";
        assert!(ring.add(node, 2).is_none());
        assert_eq!(ring.add(node_2, 3), Some(node));

        let node_for_val_a = ring.get("abc");
        let node_for_val_b = ring.get(12345);

        assert_eq!(node_for_val_a, Some(&node_2));
        assert_eq!(node_for_val_b, Some(&node_2));

        // Because of collisions, only 1 virtual node was added
        assert_eq!(ring.remove(node_2), 1);
    }

    #[test]
    fn read_me_test() {
        let mut map: HashRing<&str, _> = HashRing::default();

        // Nodes only need to implement Hash
        // Provide a weight to define the number of virtual nodes
        map.add("10.0.0.1:1234", 10);
        map.add("10.0.0.2:1234", 10);

        // Keys also only need to implement Hash
        assert_eq!(map.get("Some key"), Some(&"10.0.0.1:1234"));
        assert_eq!(map.get("Another key"), Some(&"10.0.0.2:1234"));

        map.remove("10.0.0.2:1234");

        assert_eq!(map.get("Some key"), Some(&"10.0.0.1:1234"));
        assert_eq!(map.get("Another key"), Some(&"10.0.0.1:1234"));
    }
}
