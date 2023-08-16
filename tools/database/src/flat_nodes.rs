use borsh::BorshDeserialize;
use near_primitives::state::ValueRef;
use near_primitives::types::TrieNodesCount;
use near_store::flat::store_helper;
use near_store::{NibbleSlice, RawTrieNode, RawTrieNodeWithSize, ShardUId, Store};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::utils::{flat_head_state_root, open_rocksdb, sweat_shard};

#[derive(clap::Parser)]
pub(crate) struct CreateFlatNodesCommand {}

impl CreateFlatNodesCommand {
    pub(crate) fn run(&self, home: &Path) -> anyhow::Result<()> {
        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadWrite)?);
        let store = near_store::NodeStorage::new(rocksdb.clone()).get_hot_store();
        let shard_uid = sweat_shard();
        let root = flat_head_state_root(&store, &shard_uid);
        creator::create_flat_nodes(store, shard_uid, &root);
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
struct FlatNodeNibbles {
    data: Vec<u8>,
    len: usize,
}

impl FlatNodeNibbles {
    const ODD_SIZE_FLAG: u8 = 1;

    fn new() -> Self {
        Self { data: Vec::new(), len: 0 }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        Self { data: bytes.to_vec(), len: bytes.len() * 2 }
    }

    fn nibble_at(&self, i: usize) -> u8 {
        assert!(i < self.len);
        let byte = self.data[i / 2];
        if i % 2 == 0 {
            byte >> 4
        } else {
            byte & 15
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, nibble: u8) {
        assert!(nibble < 16);
        if self.len % 2 == 0 {
            self.data.push(nibble << 4);
        } else {
            *self.data.last_mut().unwrap() += nibble;
        }
        self.len += 1;
    }

    fn with_push(&self, nibble: u8) -> Self {
        let mut ret = self.clone();
        ret.push(nibble);
        ret
    }

    fn append_slice(&mut self, nibbles: &NibbleSlice) {
        for v in nibbles.iter() {
            self.push(v);
        }
    }

    fn append_encoded_slice(&mut self, encoded_nibble_slice: &[u8]) {
        self.append_slice(&NibbleSlice::from_encoded(encoded_nibble_slice).0);
    }

    fn with_append_encoded_slice(&self, encoded_nibble_slice: &[u8]) -> Self {
        let mut ret = self.clone();
        ret.append_encoded_slice(encoded_nibble_slice);
        ret
    }

    fn starts_with(&self, other: &Self) -> bool {
        self.len >= other.len && (0..other.len).all(|i| self.nibble_at(i) == other.nibble_at(i))
    }

    fn encode_key(&self) -> Vec<u8> {
        let is_odd_size = self.len % 2 == 1;
        let mut ret = Vec::with_capacity(self.data.len() + if is_odd_size { 0 } else { 1 });
        ret.extend_from_slice(&self.data);
        if is_odd_size {
            *ret.last_mut().unwrap() += Self::ODD_SIZE_FLAG;
        } else {
            ret.push(0);
        }
        ret
    }
}

impl Debug for FlatNodeNibbles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();
        for i in 0..self.len {
            list.entry(&self.nibble_at(i));
        }
        list.finish()
    }
}

mod creator {
    use borsh::BorshDeserialize;
    use crossbeam::channel;
    use near_primitives::hash::CryptoHash;
    use near_store::flat::store_helper;
    use near_store::trie::Children;
    use near_store::{
        RawTrieNode, RawTrieNodeWithSize, ShardUId, Store, StoreUpdate, TrieDBStorage, TrieStorage,
    };

    use super::FlatNodeNibbles;

    struct ReadNodeRequest {
        path: FlatNodeNibbles,
        hash: CryptoHash,
    }

    struct ReadNodeResponse {
        path: FlatNodeNibbles,
        node: RawTrieNodeWithSize,
    }

    const UPDATES_COMMIT_SIZE: usize = 10000;
    const READ_THREADS: usize = 10;

    struct FlatNodesCreator {
        shard_uid: ShardUId,
        store: Store,
        store_update: StoreUpdate,
        pending_update: usize,
        total_created: usize,
        send: channel::Sender<ReadNodeRequest>,
        recv: channel::Receiver<ReadNodeResponse>,
    }

    impl FlatNodesCreator {
        pub fn create_root(mut self, root: CryptoHash) {
            self.create_from_hash(root, FlatNodeNibbles::new());
            self.commit_update();
        }

        fn create_node(&mut self, path: FlatNodeNibbles, node: RawTrieNodeWithSize) {
            store_helper::set_flat_node_value(
                &mut self.store_update,
                self.shard_uid,
                path.encode_key(),
                Some(&node),
            );
            self.pending_update += 1;
            if self.pending_update == UPDATES_COMMIT_SIZE {
                self.commit_update();
            }
            match node.node {
                RawTrieNode::Leaf(_, _) => {}
                RawTrieNode::BranchNoValue(children) => self.create_children(children, path),
                RawTrieNode::BranchWithValue(_, children) => self.create_children(children, path),
                RawTrieNode::Extension(key, child) => {
                    self.create_from_hash(child, path.with_append_encoded_slice(&key));
                }
            }
        }

        fn create_children(&mut self, children: Children, path: FlatNodeNibbles) {
            let resps = self.read_nodes(
                children
                    .iter()
                    .map(|(nibble, &hash)| ReadNodeRequest { path: path.with_push(nibble), hash })
                    .collect(),
            );
            for resp in resps {
                self.create_node(resp.path, resp.node);
            }
        }

        fn commit_update(&mut self) {
            self.total_created += self.pending_update;
            let mut store_update = self.store.store_update();
            std::mem::swap(&mut self.store_update, &mut store_update);
            store_update.commit().unwrap();
            let committed = self.pending_update;
            self.pending_update = 0;
            eprintln!("Committed {committed}, total: {}M", self.total_created / 1000_000);
        }

        fn create_from_hash(&mut self, hash: CryptoHash, path: FlatNodeNibbles) {
            let resp = self.read_nodes(vec![ReadNodeRequest { path, hash }]).pop().unwrap();
            self.create_node(resp.path, resp.node);
        }

        fn read_nodes(&mut self, reqs: Vec<ReadNodeRequest>) -> Vec<ReadNodeResponse> {
            let n = reqs.len();
            for req in reqs {
                self.send.send(req).unwrap();
            }
            let mut ret = Vec::new();
            for _ in 0..n {
                ret.push(self.recv.recv().unwrap());
            }
            ret
        }
    }

    pub fn create_flat_nodes(store: Store, shard_uid: ShardUId, root: &CryptoHash) {
        let (req_send, req_recv) = channel::unbounded();
        let (resp_send, resp_recv) = channel::bounded(10);
        let mut thread_handles = Vec::new();
        for _ in 0..READ_THREADS {
            thread_handles.push(start_read_node_thread(
                store.clone(),
                shard_uid,
                req_recv.clone(),
                resp_send.clone(),
            ));
        }
        std::mem::drop(req_recv);
        std::mem::drop(resp_send);
        let creator = FlatNodesCreator{
            shard_uid,
            store_update: store.store_update(),
            store,
            pending_update: 0,
            total_created: 0,
            send: req_send,
            recv: resp_recv,
        };
        creator.create_root(*root);
        //create_nodes(store, shard_uid, root, resp_recv, req_send);
        for hdl in thread_handles {
            hdl.join().unwrap();
        }
    }

    fn start_read_node_thread(
        store: Store,
        shard_uid: ShardUId,
        recv: channel::Receiver<ReadNodeRequest>,
        send: channel::Sender<ReadNodeResponse>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let trie_storage = TrieDBStorage::new(store, shard_uid);
            while let Ok(req) = recv.recv() {
                let node_with_size = RawTrieNodeWithSize::try_from_slice(
                    &trie_storage.retrieve_raw_bytes(&req.hash).unwrap(),
                )
                .unwrap();
                send.send(ReadNodeResponse { path: req.path, node: node_with_size }).unwrap();
            }
        })
    }
}

pub struct FlatNodesTrie {
    shard_uid: ShardUId,
    store: Store,
    mem: RefCell<HashMap<Vec<u8>, RawTrieNodeWithSize>>,
    nodes_count: RefCell<TrieNodesCount>,
    elapsed_db_reads: RefCell<Vec<Duration>>,
    nodes_sizes: RefCell<Vec<usize>>,
}

pub struct NodeReadData {
    pub value_ref: Option<ValueRef>,
    pub nodes_count: TrieNodesCount,
    pub elapsed_db_reads: Vec<Duration>,
    pub nodes_sizes: Vec<usize>,
}

impl FlatNodesTrie {
    pub fn new(shard_uid: ShardUId, store: Store) -> Self {
        Self {
            shard_uid,
            store,
            mem: RefCell::new(HashMap::new()),
            nodes_count: RefCell::new(TrieNodesCount { db_reads: 0, mem_reads: 0 }),
            elapsed_db_reads: RefCell::new(Vec::new()),
            nodes_sizes: RefCell::new(Vec::new()),
        }
    }

    pub fn get_ref(&self, key: &[u8]) -> NodeReadData {
        let lookup_path = FlatNodeNibbles::from_bytes(key);
        let mut cur_path = FlatNodeNibbles::new();
        let nodes_before = self.nodes_count.borrow().clone();
        let value_ref = self.lookup(&mut cur_path, &lookup_path);
        let nodes_count = self.nodes_count.borrow().clone().checked_sub(&nodes_before).unwrap();
        let mut elapsed_db_reads = Vec::new();
        std::mem::swap(self.elapsed_db_reads.borrow_mut().as_mut(), &mut elapsed_db_reads);
        let mut nodes_sizes = Vec::new();
        std::mem::swap(self.nodes_sizes.borrow_mut().as_mut(), &mut nodes_sizes);
        NodeReadData { value_ref, nodes_count, elapsed_db_reads, nodes_sizes }
    }

    fn get_node(&self, path: &FlatNodeNibbles) -> RawTrieNodeWithSize {
        let key = path.encode_key();
        let mut nodes = self.nodes_count.borrow_mut();
        let mut mem = self.mem.borrow_mut();
        if let Some(node) = mem.get(&key) {
            nodes.mem_reads += 1;
            node.clone()
        } else {
            nodes.db_reads += 1;
            let read_start = Instant::now();
            let node_bytes = store_helper::get_flat_node(&self.store, self.shard_uid, &key).unwrap().unwrap();
            self.elapsed_db_reads.borrow_mut().push(read_start.elapsed());
            self.nodes_sizes.borrow_mut().push(node_bytes.len());
            let node = RawTrieNodeWithSize::try_from_slice(&node_bytes).unwrap();
            mem.insert(key, node.clone());
            node
        }
    }

    fn lookup(
        &self,
        cur_path: &mut FlatNodeNibbles,
        lookup_path: &FlatNodeNibbles,
    ) -> Option<ValueRef> {
        //eprintln!("lookup path={lookup_path:?}, cur={cur_path:?}");
        if !lookup_path.starts_with(cur_path) {
            return None;
        }
        let node = self.get_node(cur_path).node;
        match node {
            RawTrieNode::Leaf(key, value_ref) => {
                cur_path.append_encoded_slice(&key);
                if cur_path == lookup_path {
                    Some(value_ref)
                } else {
                    None
                }
            }
            RawTrieNode::BranchNoValue(children) => {
                if cur_path.len() < lookup_path.len() {
                    self.lookup_children(cur_path, lookup_path, children)
                } else {
                    None
                }
            }
            RawTrieNode::BranchWithValue(value_ref, children) => {
                if cur_path.len() < lookup_path.len() {
                    self.lookup_children(cur_path, lookup_path, children)
                } else {
                    Some(value_ref)
                }
            }
            RawTrieNode::Extension(key, _) => {
                cur_path.append_encoded_slice(&key);
                self.lookup(cur_path, lookup_path)
            }
        }
    }

    fn lookup_children(
        &self,
        cur_path: &mut FlatNodeNibbles,
        lookup_path: &FlatNodeNibbles,
        children: near_store::trie::Children,
    ) -> Option<ValueRef> {
        let next_nibble = lookup_path.nibble_at(cur_path.len());
        if children[next_nibble].is_some() {
            cur_path.push(next_nibble);
            self.lookup(cur_path, lookup_path)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use near_store::test_utils::{create_tries, test_populate_trie};
    use near_store::{ShardUId, Trie, TrieUpdate};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use crate::flat_nodes::creator::create_flat_nodes;

    use super::FlatNodesTrie;

    #[test]
    fn flat_nodes_basic() {
        check(vec![vec![0, 1], vec![1, 0]]);
    }

    #[test]
    fn flat_nodes_rand_small() {
        check_random(3, 20, 10000);
    }

    #[test]
    fn flat_nodes_rand_many_keys() {
        check_random(5, 1000, 100);
    }

    #[test]
    fn flat_nodes_rand_long_keys() {
        check_random(20, 100, 1000);
    }

    #[test]
    fn flat_nodes_rand_large_data() {
        check_random(32, 100000, 1);
    }

    fn check_random(max_key_len: usize, max_keys_count: usize, test_count: usize) {
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..test_count {
            let key_cnt = rng.gen_range(1..=max_keys_count);
            let mut keys = Vec::new();
            for _ in 0..key_cnt {
                let mut key = Vec::new();
                let key_len = rng.gen_range(0..=max_key_len);
                for _ in 0..key_len {
                    let byte: u8 = rng.gen();
                    key.push(byte);
                }
                keys.push(key);
            }
            check(keys);
        }
    }

    fn check(keys: Vec<Vec<u8>>) {
        let description =
            if keys.len() <= 20 { format!("{keys:?}") } else { format!("{} keys", keys.len()) };
        eprintln!("TEST CASE {description}");
        let shard_tries = create_tries();
        let shard_uid = ShardUId::single_shard();
        let state_root = test_populate_trie(
            &shard_tries,
            &Trie::EMPTY_ROOT,
            shard_uid,
            keys.iter().map(|key| (key.to_vec(), Some(key.to_vec()))).collect(),
        );
        let trie_update = TrieUpdate::new(shard_tries.get_trie_for_shard(shard_uid, state_root));
        trie_update.set_trie_cache_mode(near_primitives::types::TrieCacheMode::CachingChunk);
        let trie = trie_update.trie();
        create_flat_nodes(shard_tries.get_store(), shard_uid, &state_root);
        eprintln!("flat nodes created");
        let flat_trie = FlatNodesTrie::new(shard_uid, shard_tries.get_store());
        for key in keys.iter() {
            let flat_data = flat_trie.get_ref(key);
            let nodes_before = trie.get_trie_nodes_count();
            let expected_value_ref = trie.get_ref(key, near_store::KeyLookupMode::Trie).unwrap();
            let expected_trie_nodes =
                trie.get_trie_nodes_count().checked_sub(&nodes_before).unwrap();
            assert_eq!(flat_data.value_ref, expected_value_ref);
            assert_eq!(flat_data.nodes_count, expected_trie_nodes);
        }
    }
}
