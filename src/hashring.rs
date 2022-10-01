use std::borrow::Borrow;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::num::NonZeroU64;
use std::sync::Arc;

#[derive(Debug)]
struct MasterNode<N> {
    node: N,
    weight: NonZeroU64,
}

#[derive(Debug)]
pub struct HashRing<N> {
    virtual_nodes: BTreeMap<u64, Arc<MasterNode<N>>>,
}

impl<N> Default for HashRing<N> {
    fn default() -> Self {
        Self {
            virtual_nodes: Default::default(),
        }
    }
}

impl<N> HashRing<N>
where
    N: Hash,
{
    pub fn new() -> Self {
        Self {
            virtual_nodes: BTreeMap::new(),
        }
    }

    /// There can be hash collisions resulting in fewer than weights nodes added.
    pub fn add(&mut self, node: N, weight: NonZeroU64) {
        let virtual_node_hashes = Self::compute_virtual_node_hashes(&node, weight);
        let master_node = Arc::new(MasterNode { node, weight });
        for virtual_node_hash in virtual_node_hashes.into_iter() {
            self.virtual_nodes
                .insert(virtual_node_hash, master_node.clone());
        }
    }

    #[inline]
    pub fn get_by_key<K>(&self, key: K) -> Option<&N>
    where
        K: Hash,
    {
        let mut hasher = DefaultHasher::new();
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

    fn compute_virtual_node_hashes(node: &N, weight: NonZeroU64) -> Vec<u64> {
        (0..weight.get())
            .into_iter()
            .map(|virtual_node_identifier| {
                let mut hasher = DefaultHasher::new();
                node.hash(&mut hasher);
                hasher.write_u64(virtual_node_identifier);
                // It could be that we have a collision here and therefore fewer virtual nodes
                // TODO what's the distribution here?
                hasher.finish()
            })
            .collect()
    }

    pub fn remove(&mut self, node: N) -> u64 {
        // At least one node should exist
        let virtual_node_hashes =
            Self::compute_virtual_node_hashes(&node, NonZeroU64::new(1).unwrap());
        let one_node = virtual_node_hashes
            .first()
            .and_then(|hash| self.get_master_node_by_hash(hash));
        match one_node {
            Some(master_node) => {
                let mut removed_nodes = 0;
                for virtual_node_hash in
                    Self::compute_virtual_node_hashes(&master_node.node, master_node.weight).iter()
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
        let mut ring: HashRing<&str> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.add(node, NonZeroU64::new(1).unwrap());

        assert_eq!(ring.virtual_nodes.len(), 1);
    }

    #[test]
    fn adding_a_node_with_many_virtual_nodes_works() {
        let mut ring: HashRing<&str> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.add(node, NonZeroU64::new(100).unwrap());

        assert_eq!(ring.virtual_nodes.len(), 100);
    }

    #[test]
    fn adding_multiple_nodes_with_many_virtual_nodes_works() {
        let mut ring: HashRing<&str> = HashRing::new();
        let node_1 = "10.0.0.1:12345";
        let node_2 = "20.0.0.1:12345";
        ring.add(node_1, NonZeroU64::new(100).unwrap());
        ring.add(node_2, NonZeroU64::new(500).unwrap());

        assert_eq!(ring.virtual_nodes.len(), 600);
    }

    #[test]
    fn removing_a_node_works() {
        let mut ring: HashRing<&str> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.add(node, NonZeroU64::new(1).unwrap());
        assert_eq!(ring.virtual_nodes.len(), 1);

        ring.remove(node);
        assert_eq!(ring.virtual_nodes.len(), 0);
    }

    #[test]
    fn removing_a_node_with_many_virtual_nodes_works() {
        let mut ring: HashRing<&str> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.add(node, NonZeroU64::new(100).unwrap());
        assert_eq!(ring.virtual_nodes.len(), 100);

        let nodes_removed = ring.remove(node);
        assert_eq!(nodes_removed, 100);
        assert_eq!(ring.virtual_nodes.len(), 0);
    }

    #[test]
    fn removing_multiple_nodes_with_many_virtual_nodes_works() {
        let mut ring: HashRing<&str> = HashRing::new();
        let node_1 = "10.0.0.1:12345";
        let node_2 = "20.0.0.1:12345";
        ring.add(node_1, NonZeroU64::new(100).unwrap());
        ring.add(node_2, NonZeroU64::new(500).unwrap());
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
        let mut ring: HashRing<&str> = HashRing::new();
        let node = "10.0.0.1:12345";
        ring.add(node, NonZeroU64::new(1).unwrap());

        let node_for_val_a = ring.get_by_key("abc");
        let node_for_val_b = ring.get_by_key(12345);

        assert_eq!(node_for_val_a, Some(&node));
        assert_eq!(node_for_val_b, Some(&node));
    }

    #[test]
    fn adding_multiple_nodes_and_getting_works() {
        let mut ring: HashRing<&str> = HashRing::new();
        let node_1 = "10.0.0.1:12345";
        let node_2 = "20.0.0.1:12345";
        let node_3 = "30.0.0.1:12345";
        ring.add(node_1, NonZeroU64::new(1).unwrap());
        ring.add(node_2, NonZeroU64::new(1).unwrap());
        ring.add(node_3, NonZeroU64::new(1).unwrap());

        let node_for_val_a = ring.get_by_key("hula");
        let node_for_val_b = ring.get_by_key(12345);
        let node_for_val_c = ring.get_by_key(54321);
        let node_for_val_d = ring.get_by_key(b"12345");

        assert_eq!(node_for_val_a, Some(&node_3));
        assert_eq!(node_for_val_b, Some(&node_2));
        assert_eq!(node_for_val_c, Some(&node_3));
        assert_eq!(node_for_val_d, Some(&node_1));
    }

    #[test]
    fn adding_multiple_nodes_getting_and_removing_works() {
        let mut ring: HashRing<&str> = HashRing::new();
        let node_1 = "10.0.0.1:12345";
        let node_2 = "20.0.0.1:12345";
        let node_3 = "30.0.0.1:12345";
        ring.add(node_1, NonZeroU64::new(1).unwrap());
        ring.add(node_2, NonZeroU64::new(1).unwrap());
        ring.add(node_3, NonZeroU64::new(1).unwrap());

        let key_1 = "hula";
        let key_2 = 12345;
        let key_3 = 54321;
        let key_4 = b"12345";

        {
            let node_for_key_1 = ring.get_by_key(key_1);
            let node_for_val_2 = ring.get_by_key(key_2);
            let node_for_key_3 = ring.get_by_key(key_3);
            let node_for_key_4 = ring.get_by_key(key_4);

            assert_eq!(node_for_key_1, Some(&node_3));
            assert_eq!(node_for_val_2, Some(&node_2));
            assert_eq!(node_for_key_3, Some(&node_3));
            assert_eq!(node_for_key_4, Some(&node_1));
        }

        ring.remove(node_1);

        {
            let node_for_key_1 = ring.get_by_key(key_1);
            let node_for_key_2 = ring.get_by_key(key_2);
            let node_for_key_3 = ring.get_by_key(key_3);
            let node_for_key_4 = ring.get_by_key(key_4);

            assert_eq!(node_for_key_1, Some(&node_3));
            assert_eq!(node_for_key_2, Some(&node_2));
            assert_eq!(node_for_key_3, Some(&node_3));
            // Only this value got remapped
            assert_eq!(node_for_key_4, Some(&node_2));
        }
    }
}
