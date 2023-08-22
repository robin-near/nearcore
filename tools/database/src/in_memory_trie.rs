use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use borsh::{BorshDeserialize, BorshSerialize};
use near_epoch_manager::EpochManager;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state::ValueRef;
use near_primitives::types::ShardId;
use near_store::{DBCol, RawTrieNode, RawTrieNodeWithSize, ShardUId, Store};
use nearcore::NearConfig;

use crate::flat_nodes::FlatNodeNibbles;
use crate::utils::{flat_head, flat_head_state_root, open_rocksdb};

struct InMemoryTrieNodeLite {
    hash: CryptoHash,
    size: u64,
    kind: InMemoryTrieNodeKindLite,
}

enum InMemoryTrieNodeKindLite {
    Leaf(Box<[u8]>, ValueRef),
    Extension(Box<[u8]>, Box<InMemoryTrieNodeLite>),
    Branch([Option<Box<InMemoryTrieNodeLite>>; 16]),
    BranchWithLeaf { children: [Option<Box<InMemoryTrieNodeLite>>; 16], value: ValueRef },
}

struct InMemoryTrieNodeBuilder {
    hash: CryptoHash,
    size: u64,
    leaf: Option<ValueRef>,
    extension: Option<Box<[u8]>>,
    children: [Option<Box<InMemoryTrieNodeLite>>; 16],
    expected_children: Vec<Option<CryptoHash>>,
    next_child_index: usize,
}

impl InMemoryTrieNodeBuilder {
    pub fn from_raw(node: RawTrieNodeWithSize) -> InMemoryTrieNodeBuilder {
        let mut builder = InMemoryTrieNodeBuilder {
            hash: hash(&node.try_to_vec().unwrap()),
            size: node.memory_usage,
            leaf: None,
            extension: None,
            children: Default::default(),
            expected_children: Vec::new(),
            next_child_index: 0,
        };
        match node.node {
            RawTrieNode::Leaf(extension, leaf) => {
                builder.extension = Some(extension.into_boxed_slice());
                builder.leaf = Some(leaf);
            }
            RawTrieNode::Extension(extension, node) => {
                builder.extension = Some(extension.into_boxed_slice());
                builder.expected_children.push(Some(node));
            }
            RawTrieNode::BranchNoValue(children) => {
                let mut first_child_index = None;
                for (i, child) in children.0.iter().enumerate() {
                    if child.is_some() && first_child_index.is_none() {
                        first_child_index = Some(i);
                    }
                    builder.expected_children.push(*child);
                }
                builder.next_child_index = first_child_index.unwrap_or(16);
            }
            RawTrieNode::BranchWithValue(leaf, children) => {
                builder.leaf = Some(leaf);
                let mut first_child_index = None;
                for (i, child) in children.0.iter().enumerate() {
                    if child.is_some() && first_child_index.is_none() {
                        first_child_index = Some(i);
                    }
                    builder.expected_children.push(*child);
                }
                builder.next_child_index = first_child_index.unwrap_or(16);
            }
        }
        builder
    }

    pub fn build(mut self) -> Box<InMemoryTrieNodeLite> {
        Box::new(InMemoryTrieNodeLite {
            hash: self.hash,
            size: self.size,
            kind: match (self.leaf, self.extension) {
                (Some(leaf), Some(extension)) => InMemoryTrieNodeKindLite::Leaf(extension, leaf),
                (None, Some(extension)) => {
                    assert_eq!(
                        self.next_child_index, 1,
                        "{:?}: Expected 1 child, found {}",
                        self.hash, self.next_child_index
                    );
                    InMemoryTrieNodeKindLite::Extension(
                        extension,
                        std::mem::take(&mut self.children[0]).unwrap(),
                    )
                }
                (None, None) => {
                    assert_eq!(
                        self.next_child_index, 16,
                        "{:?}: Expected 16 children, found {}",
                        self.hash, self.next_child_index
                    );
                    InMemoryTrieNodeKindLite::Branch(self.children)
                }
                _ => panic!("Invalid trie node"),
            },
        })
    }

    pub fn add_child(&mut self, child: Box<InMemoryTrieNodeLite>) {
        assert!(
            self.next_child_index < self.expected_children.len(),
            "Too many children; expected {}, actual index {}",
            self.expected_children.len(),
            self.next_child_index
        );
        assert_eq!(
            self.expected_children[self.next_child_index],
            Some(child.hash),
            "Expected child {:?}, found {:?}",
            self.expected_children[self.next_child_index],
            child.hash
        );
        self.children[self.next_child_index] = Some(child);
        self.next_child_index += 1;
        while self.next_child_index < self.expected_children.len()
            && self.expected_children[self.next_child_index].is_none()
        {
            self.next_child_index += 1;
        }
    }

    pub fn is_complete(&self) -> bool {
        self.next_child_index == self.expected_children.len()
    }
}

struct InMemoryTrieBuilderFromFlatNodes {}

impl InMemoryTrieBuilderFromFlatNodes {
    pub fn new() -> InMemoryTrieBuilderFromFlatNodes {
        InMemoryTrieBuilderFromFlatNodes {}
    }

    pub fn load_trie(
        &self,
        store: &Store,
        shard_uid: ShardUId,
        state_root: CryptoHash,
    ) -> anyhow::Result<Box<InMemoryTrieNodeLite>> {
        let mut node_stack = Vec::<InMemoryTrieNodeBuilder>::new();
        let mut root: Option<Box<InMemoryTrieNodeLite>> = None;
        let mut flatten = |node_stack: &mut Vec<InMemoryTrieNodeBuilder>,
                           root: &mut Option<Box<InMemoryTrieNodeLite>>| {
            while !node_stack.is_empty() && node_stack.last().unwrap().is_complete() {
                let child = node_stack.pop().unwrap().build();
                if node_stack.is_empty() {
                    assert!(root.is_none(), "Root already set");
                    *root = Some(child);
                    break;
                } else {
                    node_stack.last_mut().unwrap().add_child(child);
                }
            }
        };
        let mut last_print = Instant::now();
        let mut nodes_iterated = 0;
        for item in store.iter_prefix(DBCol::FlatNodes, &shard_uid.try_to_vec().unwrap()) {
            let item = item?;
            // let key = FlatNodeNibbles::from_bytes(item.0.as_ref()[8..]);
            let node = RawTrieNodeWithSize::try_from_slice(item.1.as_ref())?;
            let builder = InMemoryTrieNodeBuilder::from_raw(node);
            flatten(&mut node_stack, &mut root);
            if node_stack.is_empty() && builder.hash != state_root {
                anyhow::bail!(
                    "Root hash mismatch: expected {:?}, found {:?}",
                    state_root,
                    builder.hash
                );
            }
            node_stack.push(builder);

            nodes_iterated += 1;
            if Instant::now() - last_print > Duration::from_secs(5) {
                println!(
                    "Loaded {} nodes, current stack depth {}",
                    nodes_iterated,
                    node_stack.len(),
                );
                last_print = Instant::now();
            }
        }
        flatten(&mut node_stack, &mut root);
        assert!(node_stack.is_empty(), "Node stack not empty");
        println!("Loaded {} nodes", nodes_iterated);
        Ok(root.unwrap())
    }
}

#[derive(clap::Parser)]
pub struct InMemoryTrieCmd {
    #[clap(long)]
    shard_id: ShardId,
}

impl InMemoryTrieCmd {
    pub fn run(&self, near_config: NearConfig, home: &Path) -> anyhow::Result<()> {
        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadOnly)?);
        let store = near_store::NodeStorage::new(rocksdb.clone()).get_hot_store();
        let genesis_config = &near_config.genesis.config;
        let head = flat_head(&store);
        let block_header = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, &head.try_to_vec().unwrap())?
            .ok_or_else(|| anyhow::anyhow!("Block header not found"))?;
        let epoch_manager =
            EpochManager::new_from_genesis_config(store.clone(), &genesis_config).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(block_header.epoch_id()).unwrap();

        let shard_uid = ShardUId::from_shard_id_and_layout(self.shard_id, &shard_layout);
        let state_root = flat_head_state_root(&store, &shard_uid);

        let mut builder = InMemoryTrieBuilderFromFlatNodes::new();
        let trie = builder.load_trie(&store, shard_uid, state_root)?;
        for _ in 0..1000000 {
            std::thread::sleep(Duration::from_secs(100));
        }
        Ok(())
    }
}
