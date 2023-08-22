use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use borsh::{BorshDeserialize, BorshSerialize};
use near_epoch_manager::EpochManager;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state::ValueRef;
use near_primitives::types::ShardId;
use near_store::{DBCol, NibbleSlice, RawTrieNode, RawTrieNodeWithSize, ShardUId, Store};
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
    path: FlatNodeNibbles,
    hash_and_size: Option<(CryptoHash, u64)>,
    leaf: Option<ValueRef>,
    extension: Option<Box<[u8]>>,
    children: [Option<Box<InMemoryTrieNodeLite>>; 16],
    expected_children: Vec<Option<CryptoHash>>,
    next_child_index: usize,
    placeholder_length: Option<usize>,
    pending_children: Vec<Box<InMemoryTrieNodeLite>>,
}

impl InMemoryTrieNodeBuilder {
    pub fn placeholder(
        path: FlatNodeNibbles,
        placeholder_length: usize,
    ) -> InMemoryTrieNodeBuilder {
        InMemoryTrieNodeBuilder {
            path,
            hash_and_size: None,
            leaf: None,
            extension: None,
            children: Default::default(),
            expected_children: Vec::new(),
            next_child_index: 0,
            placeholder_length: Some(placeholder_length),
            pending_children: Vec::new(),
        }
    }

    pub fn from_raw_node(
        path: FlatNodeNibbles,
        node: RawTrieNodeWithSize,
    ) -> InMemoryTrieNodeBuilder {
        let mut builder = InMemoryTrieNodeBuilder::placeholder(path, 0);
        builder.set_raw_node(node);
        builder
    }

    pub fn set_raw_node(&mut self, node: RawTrieNodeWithSize) {
        assert!(self.placeholder_length.is_some());
        self.placeholder_length = None;
        self.hash_and_size = Some((hash(&node.try_to_vec().unwrap()), node.memory_usage));
        match node.node {
            RawTrieNode::Leaf(extension, leaf) => {
                self.extension = Some(extension.into_boxed_slice());
                self.leaf = Some(leaf);
            }
            RawTrieNode::Extension(extension, node) => {
                self.extension = Some(extension.into_boxed_slice());
                self.expected_children.push(Some(node));
            }
            RawTrieNode::BranchNoValue(children) => {
                let mut first_child_index = None;
                for (i, child) in children.0.iter().enumerate() {
                    if child.is_some() && first_child_index.is_none() {
                        first_child_index = Some(i);
                    }
                    self.expected_children.push(*child);
                }
                self.next_child_index = first_child_index.unwrap_or(16);
            }
            RawTrieNode::BranchWithValue(leaf, children) => {
                self.leaf = Some(leaf);
                let mut first_child_index = None;
                for (i, child) in children.0.iter().enumerate() {
                    if child.is_some() && first_child_index.is_none() {
                        first_child_index = Some(i);
                    }
                    self.expected_children.push(*child);
                }
                self.next_child_index = first_child_index.unwrap_or(16);
            }
        }
        let children = std::mem::take(&mut self.pending_children);
        for child in children {
            self.add_child(child);
        }
    }

    pub fn build(mut self) -> Box<InMemoryTrieNodeLite> {
        assert!(self.placeholder_length.is_none());
        Box::new(InMemoryTrieNodeLite {
            hash: self.hash_and_size.unwrap().0,
            size: self.hash_and_size.unwrap().1,
            kind: match (self.leaf, self.extension) {
                (Some(leaf), Some(extension)) => InMemoryTrieNodeKindLite::Leaf(extension, leaf),
                (None, Some(extension)) => {
                    assert_eq!(
                        self.next_child_index,
                        1,
                        "{:?}: Expected 1 child, found {}",
                        self.hash_and_size.unwrap().0,
                        self.next_child_index
                    );
                    InMemoryTrieNodeKindLite::Extension(
                        extension,
                        std::mem::take(&mut self.children[0]).unwrap(),
                    )
                }
                (None, None) => {
                    assert_eq!(
                        self.next_child_index,
                        16,
                        "{:?}: Expected 16 children, found {}",
                        self.hash_and_size.unwrap().0,
                        self.next_child_index
                    );
                    InMemoryTrieNodeKindLite::Branch(self.children)
                }
                (Some(leaf), None) => {
                    assert_eq!(
                        self.next_child_index,
                        16,
                        "{:?}: Expected 16 children, found {}",
                        self.hash_and_size.unwrap().0,
                        self.next_child_index
                    );
                    InMemoryTrieNodeKindLite::BranchWithLeaf {
                        children: self.children,
                        value: leaf,
                    }
                }
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
            "Expected child {:?} at index {}, found {:?}. Expected children: {:?}",
            self.expected_children[self.next_child_index],
            self.next_child_index,
            child.hash,
            self.expected_children
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

    pub fn child_path_length(&self) -> usize {
        if let Some(len) = self.placeholder_length {
            self.path.len() + len
        } else {
            if let Some(ext) = &self.extension {
                self.path.len() + NibbleSlice::from_encoded(ext.as_ref()).0.len()
            } else {
                self.path.len() + 1
            }
        }
    }

    pub fn split_placeholder(&mut self, child_path: FlatNodeNibbles) -> InMemoryTrieNodeBuilder {
        assert!(self.placeholder_length.is_some());
        assert!(self.child_path_length() > child_path.len());
        let new_placeholder_len = self.child_path_length() - child_path.len();
        let mut new_builder = InMemoryTrieNodeBuilder::placeholder(child_path, new_placeholder_len);
        new_builder.pending_children = std::mem::take(&mut self.pending_children);
        new_builder
    }
}

fn calculate_first_child_path(
    parent: &FlatNodeNibbles,
    raw_node: &RawTrieNodeWithSize,
) -> FlatNodeNibbles {
    let mut result = parent.clone();
    match &raw_node.node {
        RawTrieNode::Leaf(extension, _) | RawTrieNode::Extension(extension, _) => {
            result.append_encoded_slice(&extension)
        }
        RawTrieNode::BranchNoValue(children) | RawTrieNode::BranchWithValue(_, children) => {
            let index = children
                .0
                .iter()
                .enumerate()
                .find(|(_, child)| child.is_some())
                .map(|(i, _)| i)
                .expect("Branch has no children");
            result.push(index as u8);
        }
    }
    result
}

struct BuilderStack {
    stack: Vec<InMemoryTrieNodeBuilder>,
    root: Option<Box<InMemoryTrieNodeLite>>,
}

impl BuilderStack {
    pub fn new() -> BuilderStack {
        BuilderStack { stack: Vec::new(), root: None }
    }

    pub fn print(&self) {
        for (i, builder) in self.stack.iter().enumerate() {
            println!("{}: {:?} {:?}", i, builder.path, builder.placeholder_length);
        }
    }

    pub fn add_node(&mut self, path: FlatNodeNibbles, raw_node: RawTrieNodeWithSize) {
        while !self.stack.is_empty() {
            let top = self.stack.last().unwrap();
            if top.path.is_prefix_of(&path) {
                break;
            } else if !path.is_prefix_of(&top.path) {
                self.pop();
            } else {
                break;
            }
        }

        // Let's separate out the parts of the stack that are longer than the path
        // later we'll put them back.
        let mut top_part = Vec::new();
        while !self.stack.is_empty() {
            let top = self.stack.last().unwrap();
            if path.is_prefix_of(&top.path) && &path != &top.path {
                top_part.push(self.stack.pop().unwrap());
            } else {
                break;
            }
        }

        if self.stack.is_empty() {
            // this is the root node.
            assert!(top_part.is_empty());
            assert_eq!(path.len(), 0);
            self.stack.push(InMemoryTrieNodeBuilder::from_raw_node(path, raw_node));
            return;
        }

        // Now look at the top of the stack. There are three cases:
        //  1. The top of the stack is exactly the desired path, just return that.
        //  2. The top of the stack is not the desired path, but the desired path
        //     is exactly what should be placed on top of the stack. In this case,
        //     we add a new node to the stack and return that.
        //  3. The top of the stack is not the desired path, and the desired path
        //     is shorter than what should be placed on the stack. In this case,
        //     we need to split the top of the stack into two nodes.
        let top = self.stack.last_mut().unwrap();
        if &path == &top.path {
            assert!(
                top.placeholder_length.is_some(),
                "Top of the stack should be a placeholder when inserting a node at that exact path"
            );
            let first_child_path = calculate_first_child_path(&path, &raw_node);
            assert!(
                first_child_path.len() <= top.child_path_length(),
                "Raw node has path length {} greater than placeholder length {}",
                first_child_path.len(),
                top.child_path_length()
            );
            let longer = if first_child_path.len() < top.child_path_length() {
                Some(top.split_placeholder(first_child_path))
            } else {
                None
            };
            top.set_raw_node(raw_node);
            if let Some(longer) = longer {
                self.stack.push(longer);
            }
        } else {
            let child_path_length = top.child_path_length();
            if child_path_length <= path.len() {
                if child_path_length < path.len() {
                    // We need a new placeholder in between, and then we can add the node.
                    self.stack.push(InMemoryTrieNodeBuilder::placeholder(
                        path.prefix(child_path_length),
                        path.len() - child_path_length,
                    ));
                }
                self.stack.push(InMemoryTrieNodeBuilder::from_raw_node(path, raw_node));
                assert!(
                    top_part.is_empty(),
                    "Top part should be empty when inserting a new node at the right path"
                );
            } else {
                // The node is a placeholder that represented more than 1 node, so split it.
                assert!(top.placeholder_length.is_some(), "Top of the stack should be a placeholder when inserting a node into the middle of the path");
                let mut longer = top.split_placeholder(path);
                longer.set_raw_node(raw_node);
                self.stack.push(longer);
            }
        };
        for top in top_part.into_iter().rev() {
            self.stack.push(top);
        }
    }

    fn pop(&mut self) {
        let top = self.stack.pop().unwrap();
        let built = top.build();
        if self.stack.is_empty() {
            assert!(self.root.is_none(), "Root already set");
            self.root = Some(built);
        } else {
            self.stack.last_mut().unwrap().add_child(built);
        }
    }

    pub fn finalize(mut self) -> Box<InMemoryTrieNodeLite> {
        while !self.stack.is_empty() {
            self.pop();
        }
        self.root.unwrap()
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
        let mut node_stack = BuilderStack::new();
        let mut last_print = Instant::now();
        let mut nodes_iterated = 0;
        for item in store.iter_prefix(DBCol::FlatNodes, &shard_uid.try_to_vec().unwrap()) {
            let item = item?;
            let key = FlatNodeNibbles::from_encoded_key(&item.0.as_ref()[8..]);
            let node = RawTrieNodeWithSize::try_from_slice(item.1.as_ref())?;
            node_stack.add_node(key, node);

            nodes_iterated += 1;
            if true {
                println!("Loaded {} nodes, current stack:", nodes_iterated,);
                node_stack.print();
                last_print = Instant::now();
            }
        }
        let root = node_stack.finalize();
        println!("Loaded {} nodes", nodes_iterated);
        assert_eq!(root.hash, state_root);
        Ok(root)
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
