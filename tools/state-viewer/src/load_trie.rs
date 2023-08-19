use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use borsh::{BorshDeserialize, BorshSerialize};
use near_chain::{ChainStore, ChainStoreAccess};
use near_epoch_manager::EpochManager;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::ShardId;
use near_store::{DBCol, RawTrieNode, RawTrieNodeWithSize, ShardUId, Store};
use nearcore::NearConfig;

#[derive(clap::Parser)]
pub struct LoadTrieCmd {
    #[clap(long)]
    shard_id: ShardId,
}

impl LoadTrieCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        let genesis_config = &near_config.genesis.config;
        let chain_store = ChainStore::new(
            store.clone(),
            genesis_config.genesis_height,
            near_config.client_config.save_trie_changes,
        );
        let head = chain_store.head().unwrap();
        let block = chain_store.get_block(&head.last_block_hash).unwrap();
        let epoch_manager =
            EpochManager::new_from_genesis_config(store.clone(), &genesis_config).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();

        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            if chunk_header.height_included() != block.header().height() {
                println!("chunk for shard {shard_id} is missing and will be skipped");
            }
        }

        let state_root = block.chunks()[self.shard_id as usize].prev_state_root();
        let shard_uid = ShardUId::from_shard_id_and_layout(self.shard_id, &shard_layout);
        let mut builder = InMemoryTrieBuilder::new();
        let trie = builder.load_trie(&store, shard_uid, &state_root).unwrap();
        for _ in 0..1000000 {
            std::thread::sleep(Duration::from_secs(100));
        }
        drop(trie);
    }
}

struct InMemoryTrieBuilder {
    pending_data: HashMap<CryptoHash, Box<[u8]>>,
    pending_nodes: HashMap<CryptoHash, Rc<InMemoryTrieNode>>,
    pending_values: HashMap<CryptoHash, Rc<InMemoryTrieValue>>,
    num_nodes_in_trie: usize,
    num_values_in_trie: usize,
    last_printed: Instant,
    depth: u32,
}

impl InMemoryTrieBuilder {
    fn new() -> Self {
        Self {
            pending_data: HashMap::new(),
            pending_nodes: HashMap::new(),
            pending_values: HashMap::new(),
            num_nodes_in_trie: 0,
            num_values_in_trie: 0,
            last_printed: Instant::now(),
            depth: 0,
        }
    }

    fn load_trie(
        &mut self,
        store: &Store,
        shard_uid: ShardUId,
        state_root: &CryptoHash,
    ) -> anyhow::Result<Rc<InMemoryTrieNode>> {
        let mut last_print = Instant::now();
        for item in store.iter_prefix(DBCol::State, &shard_uid.try_to_vec().unwrap()) {
            let (key, value) = item.unwrap();
            assert_eq!(key.len(), 40);
            let hash = CryptoHash::try_from(&key[8..]).unwrap();
            // println!("Loaded node {:?} -> {}", hash, hex::encode(value.as_ref()));
            self.pending_data.insert(hash, value);
            if Instant::now().duration_since(last_print) > std::time::Duration::from_secs(10) {
                println!("Loaded {} nodes...", self.pending_data.len());
                last_print = Instant::now();
            }
        }
        println!("Loaded {} nodes total. Now constructing the trie...", self.pending_data.len());

        let trie = self.construct_trie_node(state_root)?;
        self.pending_data.clear();
        self.pending_nodes.clear();
        self.pending_values.clear();
        println!(
            "Constructed a trie of {} nodes and {} values.",
            self.num_nodes_in_trie, self.num_values_in_trie
        );
        Ok(trie)
    }

    fn construct_trie_node(&mut self, hash: &CryptoHash) -> anyhow::Result<Rc<InMemoryTrieNode>> {
        // println!("[{}] Trying to construct trie node {:?}", self.depth, hash);
        self.depth += 1;
        let result = match self.pending_nodes.entry(*hash) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(_) => match self.pending_data.entry(*hash) {
                Entry::Occupied(entry) => {
                    let (key, value) = entry.remove_entry();
                    let node =
                        RawTrieNodeWithSize::try_from_slice(value.as_ref()).map_err(|err| {
                            anyhow::anyhow!("Failed to parse trie node {:?}: {}", hash, err)
                        })?;
                    let in_memory_node_kind = match node.node {
                        RawTrieNode::Leaf(path, value_ref) => InMemoryTrieNodeKind::Leaf(
                            path.into_boxed_slice(),
                            self.construct_trie_value(&value_ref.hash)?,
                        ),
                        RawTrieNode::BranchNoValue(children) => InMemoryTrieNodeKind::Branch([
                            children.0[0]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[1]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[2]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[3]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[4]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[5]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[6]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[7]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[8]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[9]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[10]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[11]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[12]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[13]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[14]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                            children.0[15]
                                .map(|hash| self.construct_trie_node(&hash))
                                .transpose()?,
                        ]),
                        RawTrieNode::BranchWithValue(value_ref, children) => {
                            InMemoryTrieNodeKind::BranchWithLeaf {
                                children: [
                                    children.0[0]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[1]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[2]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[3]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[4]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[5]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[6]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[7]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[8]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[9]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[10]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[11]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[12]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[13]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[14]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                    children.0[15]
                                        .map(|hash| self.construct_trie_node(&hash))
                                        .transpose()?,
                                ],
                                value: self.construct_trie_value(&value_ref.hash)?,
                            }
                        }
                        RawTrieNode::Extension(extension, node) => InMemoryTrieNodeKind::Extension(
                            extension.into_boxed_slice(),
                            self.construct_trie_node(&node)?,
                        ),
                    };

                    let built_entry = Rc::new(InMemoryTrieNode {
                        hash: key,
                        size: node.memory_usage,
                        kind: in_memory_node_kind,
                    });
                    self.pending_nodes.insert(key, built_entry.clone());
                    self.num_nodes_in_trie += 1;
                    if Instant::now().duration_since(self.last_printed) > Duration::from_secs(10) {
                        self.last_printed = Instant::now();
                        println!(
                            "Building trie: {} nodes, {} values",
                            self.num_nodes_in_trie, self.num_values_in_trie
                        );
                    }
                    Ok(built_entry)
                }
                Entry::Vacant(_) => Err(anyhow::anyhow!("Could not find node {:?}", hash)),
            },
        };
        self.depth -= 1;
        result
    }

    fn construct_trie_value(&mut self, hash: &CryptoHash) -> anyhow::Result<Rc<InMemoryTrieValue>> {
        // println!("[{}] Trying to construct trie value {:?}", self.depth, hash);
        match self.pending_values.entry(*hash) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(built_entry) => match self.pending_data.entry(*hash) {
                Entry::Occupied(entry) => {
                    let (key, value) = entry.remove_entry();
                    let result = InMemoryTrieValue { size: value.len(), hash: key, data: value };
                    self.num_values_in_trie += 1;
                    Ok(built_entry.insert(Rc::new(result)).clone())
                }
                Entry::Vacant(_) => Err(anyhow::anyhow!("Could not find value {:?}", hash)),
            },
        }
    }
}

struct PendingValue {
    value: Box<[u8]>,
    in_memory_node: Option<Rc<InMemoryTrieNode>>,
    in_memory_value: Option<Rc<InMemoryTrieValue>>,
}

struct InMemoryTrieNode {
    hash: CryptoHash,
    size: u64,
    kind: InMemoryTrieNodeKind,
}

enum InMemoryTrieNodeKind {
    Leaf(Box<[u8]>, Rc<InMemoryTrieValue>),
    Extension(Box<[u8]>, Rc<InMemoryTrieNode>),
    Branch([Option<Rc<InMemoryTrieNode>>; 16]),
    BranchWithLeaf { children: [Option<Rc<InMemoryTrieNode>>; 16], value: Rc<InMemoryTrieValue> },
}

struct InMemoryTrieValue {
    size: usize,
    hash: CryptoHash,
    data: Box<[u8]>,
}
