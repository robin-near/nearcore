use borsh::{BorshDeserialize, BorshSerialize};

use super::FlexibleDataHeader;
use crate::trie::mem::arena::{ArenaPtr, ArenaSlice, BorshFixedSize};
use crate::trie::mem::node::MemTrieNode;
use crate::trie::Children;

/// Flexibly-sized data header for a variable-sized list of children trie nodes.
/// The header contains a 16-bit mask of which children are present, and the
/// flexible part is one pointer for each present child.
#[derive(Clone, Copy, BorshSerialize, BorshDeserialize)]
pub struct EncodedChildrenHeader {
    mask: u16,
}

impl BorshFixedSize for EncodedChildrenHeader {
    const SERIALIZED_SIZE: usize = std::mem::size_of::<u16>();
}

impl FlexibleDataHeader for EncodedChildrenHeader {
    type InputData = Vec<Option<MemTrieNode>>;
    type View = ChildrenView;

    fn from_input(children: &Vec<Option<MemTrieNode>>) -> EncodedChildrenHeader {
        let mut mask = 0u16;
        for i in 0..16 {
            if children[i].is_some() {
                mask |= 1 << i;
            }
        }
        EncodedChildrenHeader { mask }
    }

    fn flexible_data_length(&self) -> usize {
        self.mask.count_ones() as usize * ArenaPtr::SIZE
    }

    fn encode_flexible_data(&self, children: Vec<Option<MemTrieNode>>, target: &mut ArenaSlice) {
        assert_eq!(children.len(), 16);
        let mut j = 0;
        for (i, child) in children.into_iter().enumerate() {
            if self.mask & (1 << i) != 0 {
                target.write_ptr_at(j, &child.unwrap().ptr);
                j += ArenaPtr::SIZE;
            }
        }
    }

    fn decode_flexible_data(&self, source: &ArenaSlice) -> ChildrenView {
        ChildrenView { mask: self.mask, children: source.clone() }
    }
}

/// Efficient view of the encoded children data.
#[derive(Debug, Clone)]
pub struct ChildrenView {
    mask: u16,
    children: ArenaSlice,
}

impl ChildrenView {
    /// Gets the child at a specific index (0 to 15).
    pub fn get(&self, i: usize) -> Option<MemTrieNode> {
        assert!(i < 16);
        let bit = 1u16 << (i as u16);
        if self.mask & bit == 0 {
            None
        } else {
            let lower_mask = self.mask & (bit - 1);
            let index = lower_mask.count_ones() as usize;
            Some(MemTrieNode::from(self.children.read_ptr_at(index * ArenaPtr::SIZE)))
        }
    }

    /// Converts to a Children struct used in RawTrieNode.
    pub fn to_children(&self) -> Children {
        let mut children = Children::default();
        let mut j = 0;
        for i in 0..16 {
            if self.mask & (1 << i) != 0 {
                let child = MemTrieNode::from(self.children.read_ptr_at(j));
                children.0[i] = Some(child.hash());
                j += ArenaPtr::SIZE;
            }
        }
        children
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = MemTrieNode> + 'a {
        (0..self.mask.count_ones() as usize)
            .map(|i| MemTrieNode::from(self.children.read_ptr_at(i * ArenaPtr::SIZE)))
    }
}
