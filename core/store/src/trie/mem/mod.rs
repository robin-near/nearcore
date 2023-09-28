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
    pub roots: HashMap<StateRoot, Vec<MemTrieNodeId>>,
    pub heights: BTreeMap<BlockHeight, Vec<StateRoot>>,
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
        if state_root != CryptoHash::default() {
            let heights = self.heights.entry(block_height).or_default();
            if heights.contains(&state_root) {
                panic!("INSERT ROOT {} height {} already exists", state_root, block_height);
            }
            heights.push(state_root);
            let new_ref = mem_root.add_ref(&mut self.arena);
            if new_ref == 1 {
                let roots_with_same_hash = self.roots.entry(state_root).or_default();
                roots_with_same_hash.push(mem_root);
                println!(
                    "INSERT ROOT {} height {}, dupindex {} ref {} -> {}",
                    state_root,
                    block_height,
                    roots_with_same_hash.len() - 1,
                    new_ref - 1,
                    new_ref
                );
            } else {
                println!(
                    "INSERT ROOT {} height {} ref {} -> {}",
                    state_root,
                    block_height,
                    new_ref - 1,
                    new_ref
                );
            }
        } else {
            println!("INSERT ROOT {}", state_root);
        }
        crate::metrics::MEM_TRIE_ROOTS
            .with_label_values(&[&self.shard_uid.shard_id.to_string()])
            .set(self.roots.len() as i64);
    }

    pub fn get_root<'a>(&'a self, state_root: &CryptoHash) -> Option<MemTrieNodePtr<'a>> {
        if state_root != &CryptoHash::default() {
            self.roots.get(state_root).map(|ids| ids[0].to_ref(self.arena.memory()))
        } else {
            Some(MemTrieNodeId::from(usize::MAX).to_ref(self.arena.memory()))
        }
    }

    pub fn delete_until_height(&mut self, block_height: BlockHeight) {
        let mut to_delete = vec![];
        self.heights.retain(|height, state_roots| {
            if *height < block_height {
                for state_root in state_roots {
                    to_delete.push((*height, *state_root))
                }
                false
            } else {
                true
            }
        });
        for (height, state_root) in to_delete {
            self.delete_root(&state_root, height);
        }
    }

    fn delete_root(&mut self, state_root: &CryptoHash, height: BlockHeight) {
        if let Some(ids) = self.roots.get_mut(state_root) {
            let last_id = ids.last().unwrap();
            let new_ref = last_id.remove_ref(&mut self.arena);
            println!(
                "DELETE ROOT {} height {} dupindex {} ref {} -> {}",
                state_root,
                height,
                ids.len() - 1,
                new_ref + 1,
                new_ref
            );
            if new_ref == 0 {
                ids.pop();
                if ids.is_empty() {
                    self.roots.remove(state_root);
                }
            }
        } else {
            println!("DELETE ROOT {} incorrect, no such root", state_root);
        }
        crate::metrics::MEM_TRIE_ROOTS
            .with_label_values(&[&self.shard_uid.shard_id.to_string()])
            .set(self.roots.len() as i64);
    }
}
