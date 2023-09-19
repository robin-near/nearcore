use self::arena::Arena;
use self::node::{MemTrieNodeId, MemTrieNodePtr};
use near_primitives::hash::CryptoHash;
use std::collections::HashMap;

mod arena;
mod construction;
mod flexible_data;
pub mod loading;
pub mod lookup;
pub mod node;

/// Check this, because in the code we conveniently assume usize is 8 bytes.
/// In-memory trie can't possibly work under 32-bit anyway.
#[cfg(not(target_pointer_width = "64"))]
compile_error!("In-memory trie requires a 64 bit platform");

pub struct MemTries {
    arena: Arena,
    pub roots: HashMap<CryptoHash, MemTrieNodeId>,
}

impl MemTries {
    pub fn new(arena_size_in_pages: usize) -> Self {
        Self { arena: Arena::new(arena_size_in_pages), roots: HashMap::new() }
    }

    pub fn construct_root<Error>(
        &mut self,
        state_root: CryptoHash,
        mut f: impl FnMut(&mut Arena) -> Result<MemTrieNodeId, Error>,
    ) -> Result<(), Error> {
        let root = f(&mut self.arena)?;
        root.add_ref(&mut self.arena);
        self.roots.insert(state_root, root);
        Ok(())
    }

    pub fn get_root<'a>(&'a self, state_root: &CryptoHash) -> Option<MemTrieNodePtr<'a>> {
        self.roots.get(state_root).map(|id| id.as_ptr(self.arena.memory()))
    }

    pub fn delete_root(&mut self, state_root: &CryptoHash) {
        if let Some(id) = self.roots.get(state_root) {
            id.remove_ref(&mut self.arena);
        }
        self.roots.remove(state_root);
    }
}
