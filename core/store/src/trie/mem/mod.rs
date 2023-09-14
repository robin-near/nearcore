use std::collections::HashMap;
use std::rc::Rc;

use near_primitives::hash::CryptoHash;

use self::arena::ArenaHandle;
use self::node::MemTrieNode;

mod arena;
mod construction;
mod flexible_data;
pub mod loading;
pub mod lookup;
pub mod node;

pub struct MemTries {
    arena: ArenaHandle,
    pub roots: HashMap<CryptoHash, MemTrieNode>,
}
