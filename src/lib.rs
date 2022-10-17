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
use std::collections::{BTreeMap, HashSet};
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

/// A hash ring for consistent hashing.
///
///
/// # Examples
///
/// ```
/// use hulahoop::HashRing;
/// let mut map: HashRing<&str, _> = HashRing::new();
///
/// map.insert("10.0.0.1:1234", 1);
/// assert_eq!(map.get("Some key"), Some(&"10.0.0.1:1234"));
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
    /// use hulahoop::HashRing;
    ///
    /// let mut map: HashRing<&str, _> = HashRing::new();
    ///
    /// map.insert("10.0.0.1:1234", 1);
    /// assert_eq!(map.get("Some key"), Some(&"10.0.0.1:1234"));
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
    /// use hulahoop::HashRing;
    ///
    /// let mut map: HashRing<&str, _> = HashRing::new();
    ///
    /// map.insert("10.0.0.1:1234", 1);
    /// assert_eq!(map.get("Some key"), Some(&"10.0.0.1:1234"));
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
    /// use rustc_hash::FxHasher;
    /// use hulahoop::HashRing;
    ///
    /// let mut map: HashRing<&str, BuildHasherDefault<FxHasher>> = HashRing::with_hasher(BuildHasherDefault::<FxHasher>::default());
    ///
    /// map.insert("10.0.0.1:1234", 1);
    /// assert_eq!(map.get("Some key"), Some(&"10.0.0.1:1234"));
    /// ```
    pub fn with_hasher(hash_builder: B) -> Self {
        Self {
            virtual_nodes: BTreeMap::new(),
            hash_builder,
        }
    }

    /// Returns a reference to the ring’s `BuildHasher`.
    ///
    /// # Examples
    ///
    /// ```
    /// use hulahoop::HashRing;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hasher = RandomState::new();
    /// let map: HashRing<&str, _> = HashRing::with_hasher(hasher);
    /// let hasher: &RandomState = map.hasher();
    /// ```
    pub fn hasher(&self) -> &B {
        &self.hash_builder
    }

    /// Inserts a node to the `HashRing`.
    ///
    /// A `weight`, representing the number of virtual nodes for the given `node`, must be provided.
    ///
    /// There can be hash collisions resulting in fewer than `weight` virtual nodes added.
    /// If the ring did not have this node present, None is returned.
    /// If the ring did have this node present, the virtual nodes are updated, and the old node is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use hulahoop::HashRing;
    ///
    /// let mut map: HashRing<&str, _> = HashRing::default();
    ///
    /// assert_eq!(map.insert("10.0.0.1:1234", 1), None);
    /// assert_eq!(map.insert("10.0.0.1:1234", 1), Some("10.0.0.1:1234"));
    /// ```
    pub fn insert(&mut self, node: N, weight: u64) -> Option<N> {
        if weight == 0 {
            return None;
        }
        let weight = NonZeroU64::new(weight).unwrap();
        let virtual_node_hashes = self.compute_virtual_node_hashes(&node, weight);
        let actual_weight = NonZeroU64::new(virtual_node_hashes.len() as u64).unwrap();
        let mut colliding_node = None;
        if self
            .virtual_nodes
            // It's guaranteed that at least one element is present
            .contains_key(virtual_node_hashes.iter().next().unwrap())
        {
            colliding_node = self.remove_inner(&node).0;
        }
        let master_node = Arc::new(MasterNode {
            node,
            weight: actual_weight,
        });

        for virtual_node_hash in virtual_node_hashes.into_iter() {
            self.virtual_nodes
                .insert(virtual_node_hash, master_node.clone());
        }
        colliding_node
    }

    /// Returns a reference to the node with a hash closest to the hash of the key.
    ///
    /// # Examples
    ///
    /// ```
    /// use hulahoop::HashRing;
    ///
    /// let mut map: HashRing<&str, _> = HashRing::default();
    ///
    /// map.insert("10.0.0.1:1234", 1);
    /// assert_eq!(map.get("Some key"), Some(&"10.0.0.1:1234"));
    /// assert_eq!(map.get(12345), Some(&"10.0.0.1:1234"));
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

    /// Returns the number of nodes in the Hashring.
    ///
    /// It does not return the number of virtual nodes.
    ///
    /// # Examples
    ///
    /// ```
    /// use hulahoop::HashRing;
    ///
    /// let mut map: HashRing<&str, _> = HashRing::default();
    ///
    /// map.insert("10.0.0.1:1234", 10);
    /// map.insert("10.0.0.2:1234", 10);
    /// assert_eq!(map.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        self.virtual_nodes
            .values()
            .map(|node| {
                let mut hasher = self.hash_builder.build_hasher();
                node.node.hash(&mut hasher);
                hasher.finish()
            })
            .collect::<HashSet<_>>()
            .len()
    }

    /// Returns `true` if the ring contains no elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use hulahoop::HashRing;
    ///
    /// let mut map: HashRing<&str, _> = HashRing::default();
    /// assert!(map.is_empty());
    ///
    /// map.insert("10.0.0.1:1234", 10);
    /// assert!(!map.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.virtual_nodes.is_empty()
    }

    /// Returns `true` if the ring contains the specified node.
    ///
    /// # Examples
    ///
    /// ```
    /// use hulahoop::HashRing;
    ///
    /// let mut map: HashRing<&str, _> = HashRing::default();
    ///
    /// map.insert("10.0.0.1:1234", 10);
    /// assert_eq!(map.contains_node(&"10.0.0.1:1234"), true);
    /// assert_eq!(map.contains_node(&"10.0.0.2:1234"), false);
    /// ```
    pub fn contains_node(&self, node: &N) -> bool {
        self.get_master_node(node).is_some()
    }

    fn get_master_node_by_hash(&self, hash: &u64) -> Option<&MasterNode<N>> {
        self.virtual_nodes.get(hash).map(|node| node.as_ref())
    }

    fn compute_virtual_node_hashes(&self, node: &N, weight: NonZeroU64) -> HashSet<u64> {
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
    /// The number of virtual nodes (weight) of the removed node can be lower than the weight provided
    /// when the node was inserted in case hash collisions occurred.
    ///
    /// # Examples
    ///
    /// ```
    /// use hulahoop::HashRing;
    ///
    /// let mut map: HashRing<&str, _> = HashRing::default();
    ///
    /// map.insert("10.0.0.1:1234", 10);
    /// assert_eq!(map.remove(&"10.0.0.1:1234"), 10);
    /// assert_eq!(map.remove(&"10.0.0.1:1234"), 0);
    /// ```
    pub fn remove(&mut self, node: &N) -> u64 {
        self.remove_inner(node).1
    }

    fn remove_inner(&mut self, node: &N) -> (Option<N>, u64) {
        match self.get_master_node(node) {
            Some(master_node) => {
                let mut number_of_removed_virtual_nodes = 0;
                let mut removed_node = None;
                let mut virtual_node_hashes = self
                    .compute_virtual_node_hashes(&master_node.node, master_node.weight)
                    .into_iter()
                    .peekable();
                while let Some(virtual_node_hash) = virtual_node_hashes.next() {
                    if let Some(node) = self.virtual_nodes.remove(&virtual_node_hash) {
                        number_of_removed_virtual_nodes += 1;
                        if virtual_node_hashes.peek().is_none() {
                            // Last item in iterator, there should be no other references to the master node and we should be able to get the node out of Arc.
                            let removed_node_result = Arc::try_unwrap(node);
                            removed_node =
                                removed_node_result.ok().map(|master_node| master_node.node);
                        }
                    };
                }
                (removed_node, number_of_removed_virtual_nodes)
            }
            None => (None, 0),
        }
    }

    fn get_master_node(&self, node: &N) -> Option<&MasterNode<N>> {
        // At least one node should exist
        let virtual_node_hashes =
            self.compute_virtual_node_hashes(node, NonZeroU64::new(1).unwrap());
        virtual_node_hashes
            .iter()
            .next()
            .and_then(|hash| self.get_master_node_by_hash(hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adding_a_node_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.insert(node, 1);

        assert_eq!(ring.virtual_nodes.len(), 1);
    }

    #[test]
    fn adding_a_node_with_many_virtual_nodes_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.insert(node, 100);

        assert_eq!(ring.virtual_nodes.len(), 100);
    }

    #[test]
    fn adding_multiple_nodes_with_many_virtual_nodes_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node_1 = "10.0.0.1:12345";
        let node_2 = "20.0.0.1:12345";
        ring.insert(node_1, 100);
        ring.insert(node_2, 500);

        assert_eq!(ring.virtual_nodes.len(), 600);
    }

    #[test]
    fn removing_a_node_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.insert(node, 1);
        assert_eq!(ring.virtual_nodes.len(), 1);

        ring.remove(&node);
        assert_eq!(ring.virtual_nodes.len(), 0);
    }

    #[test]
    fn removing_a_node_with_many_virtual_nodes_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.insert(node, 100);
        assert_eq!(ring.virtual_nodes.len(), 100);

        let nodes_removed = ring.remove(&node);
        assert_eq!(nodes_removed, 100);
        assert_eq!(ring.virtual_nodes.len(), 0);
    }

    #[test]
    fn removing_multiple_nodes_with_many_virtual_nodes_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node_1 = "10.0.0.1:12345";
        let node_2 = "20.0.0.1:12345";
        ring.insert(node_1, 100);
        ring.insert(node_2, 500);
        assert_eq!(ring.virtual_nodes.len(), 600);
        assert_eq!(ring.len(), 2);

        let nodes_removed = ring.remove(&node_1);
        assert_eq!(nodes_removed, 100);
        assert_eq!(ring.virtual_nodes.len(), 500);
        assert_eq!(ring.len(), 1);

        let nodes_removed = ring.remove(&node_2);
        assert_eq!(nodes_removed, 500);
        assert_eq!(ring.len(), 0);
    }

    #[test]
    fn adding_one_node_and_getting_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.insert(node, 1);

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
        ring.insert(node_1, 1);
        ring.insert(node_2, 1);
        ring.insert(node_3, 1);

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
        ring.insert(node_1, 1);
        ring.insert(node_2, 1);
        ring.insert(node_3, 1);

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

        ring.remove(&node_1);

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
        ring.insert(node, 1);

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
    fn virtual_node_collisions_is_not_an_issue() {
        let mut ring: HashRing<&str, _> =
            HashRing::with_hasher(BuildHasherDefault::<CollisionHasher>::default());
        let node = "10.0.0.1:12345";
        let node_2 = "10.0.0.2:12345";
        assert!(ring.insert(node, 3).is_none());
        assert_eq!(ring.insert(node_2, 2), Some(node));

        let node_for_val_a = ring.get("abc");
        let node_for_val_b = ring.get(12345);

        assert_eq!(node_for_val_a, Some(&node_2));
        assert_eq!(node_for_val_b, Some(&node_2));

        // Because of collisions, only 1 virtual node was added
        assert_eq!(ring.remove(&node_2), 1);
        assert!(ring.is_empty())
    }

    #[test]
    fn adding_the_same_node_twice_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node = "10.0.0.1:12345";
        assert_eq!(ring.insert(node, 5), None);
        assert_eq!(ring.insert(node, 3), Some(node));

        assert_eq!(ring.len(), 1);
    }

    #[test]
    fn contains_node_works() {
        let mut ring: HashRing<&str, _> = HashRing::new();
        let node_1 = "10.0.0.1:12345";
        ring.insert(node_1, 5);
        assert!(ring.contains_node(&node_1));

        let node_2 = "10.0.0.2:12345";
        ring.insert(node_2, 10);
        assert!(ring.contains_node(&node_2));

        ring.remove(&node_1);
        assert!(!ring.contains_node(&node_1));
        assert!(ring.contains_node(&node_2));

        ring.remove(&node_2);
        assert!(!ring.contains_node(&node_2));
    }

    #[test]
    fn read_me_test() {
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
    }
}
