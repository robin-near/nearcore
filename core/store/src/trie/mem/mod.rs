use std::collections::HashMap;

use near_primitives::hash::CryptoHash;

use self::node::MemTrieNode;

mod construction;
mod flexible_data;
pub mod loading;
pub mod lookup;
pub mod node;

pub struct MemTries {
    pub roots: HashMap<CryptoHash, MemTrieNode>,
}
