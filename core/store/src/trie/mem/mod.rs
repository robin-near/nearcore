pub use self::arena::Arena;
use self::node::{MemTrieNodeId, MemTrieNodePtr};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{BlockHeight, StateRoot};
use std::collections::{BTreeMap, HashMap};

mod arena;
mod construction;
mod flexible_data;
pub mod loading;
pub mod lookup;
pub mod node;

pub struct MemTries {
    pub arena: Arena,
    pub roots: HashMap<StateRoot, MemTrieNodeId>,
    // bad, uniqueness of heights is not guaranteed
    pub heights: BTreeMap<BlockHeight, StateRoot>,
    shard_uid: ShardUId,
}

impl MemTries {
    pub fn new(arena_size_in_pages: usize, shard_uid: ShardUId) -> Self {
        Self {
            arena: Arena::new_with(arena_size_in_pages, shard_uid),
            roots: HashMap::new(),
            heights: Default::default(),
            shard_uid,
        }
    }

    pub fn construct_root<Error>(
        &mut self,
        state_root: CryptoHash,
        block_height: BlockHeight,
        mut f: impl FnMut(&mut Arena) -> Result<MemTrieNodeId, Error>,
    ) -> Result<(), Error> {
        let root = f(&mut self.arena)?;
        self.insert_root(state_root, root, block_height);
        Ok(())
    }

    pub fn insert_root(
        &mut self,
        state_root: StateRoot,
        mem_root: MemTrieNodeId,
        block_height: BlockHeight,
    ) {
        println!("INSERT ROOT {}", state_root);
        if state_root != CryptoHash::default() {
            self.roots.insert(state_root, mem_root);
            self.heights.insert(block_height, state_root);
            mem_root.add_ref(&mut self.arena);
        }
        crate::metrics::MEM_TRIE_ROOTS
            .with_label_values(&[&self.shard_uid.shard_id.to_string()])
            .set(self.roots.len() as i64);
    }

    pub fn get_root<'a>(&'a self, state_root: &CryptoHash) -> Option<MemTrieNodePtr<'a>> {
        if state_root != &CryptoHash::default() {
            self.roots.get(state_root).map(|id| id.to_ref(self.arena.memory()))
        } else {
            Some(MemTrieNodeId::from(usize::MAX).to_ref(self.arena.memory()))
        }
    }

    pub fn delete_until_height(&mut self, block_height: BlockHeight) {
        let mut to_delete = vec![];
        for (height, state_root) in self.heights.iter() {
            if *height >= block_height {
                break;
            }
            to_delete.push((*height, *state_root));
        }

        for (height, state_root) in to_delete.into_iter() {
            self.heights.remove(&height);
            self.delete_root(&state_root);
        }
    }

    pub fn delete_root(&mut self, state_root: &CryptoHash) {
        println!("DELETE ROOT {}", state_root);
        if let Some(id) = self.roots.get(state_root) {
            let new_ref = id.remove_ref(&mut self.arena);
            if new_ref == 0 {
                // not necessarily the case if there are same roots for different chunks
                self.roots.remove(state_root);
            }
        }
        crate::metrics::MEM_TRIE_ROOTS
            .with_label_values(&[&self.shard_uid.shard_id.to_string()])
            .set(self.roots.len() as i64);
    }
}
