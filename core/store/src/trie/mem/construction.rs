use crate::trie::mem::node::InputMemTrieNode;
use crate::NibbleSlice;
use near_primitives::state::FlatStateValue;

use super::arena::Arena;
use super::node::MemTrieNodeId;

/// Algorithm to construct a trie from a given stream of sorted leaf values.
///
/// This is a bottom-up algorithm that avoids constructing trie nodes until
/// they are complete.
///
/// The algorithm maintains a list of segments, where each segment represents
/// a subpath of the last key (where path means a sequence of nibbles), and
/// the segments' subpaths join together to form the last key.
pub struct TrieConstructor<'a> {
    arena: &'a mut Arena,
    segments: Vec<TrieConstructionSegment>,
}

struct TrieConstructionSegment {
    is_branch: bool,
    trail: Vec<u8>,
    leaf: Option<FlatStateValue>,
    children: Vec<(u8, MemTrieNodeId)>,
    child: Option<MemTrieNodeId>,
}

impl TrieConstructionSegment {
    fn new_branch(initial_trail: u8) -> Self {
        Self {
            is_branch: true,
            trail: vec![0x10 + initial_trail],
            leaf: None,
            children: Vec::new(),
            child: None,
        }
    }

    fn new_extension(trail: Vec<u8>) -> Self {
        let nibbles = NibbleSlice::from_encoded(&trail);
        assert!(nibbles.1 || !nibbles.0.is_empty());
        Self { is_branch: false, trail, leaf: None, children: Vec::new(), child: None }
    }

    fn is_leaf(&self) -> bool {
        self.leaf.is_some() && !self.is_branch
    }

    fn into_node(self, arena: &mut Arena) -> MemTrieNodeId {
        let input_node = if self.is_branch {
            assert!(!self.children.is_empty());
            assert!(self.child.is_none());
            let mut children: Vec<Option<MemTrieNodeId>> = Vec::new();
            children.resize_with(16, || None);
            for (i, child) in self.children.into_iter() {
                children[i as usize] = Some(child);
            }
            if let Some(leaf) = self.leaf {
                InputMemTrieNode::BranchWithValue { children, value: leaf }
            } else {
                InputMemTrieNode::Branch { children }
            }
        } else if let Some(leaf) = self.leaf {
            assert!(self.child.is_none());
            assert!(self.children.is_empty());
            InputMemTrieNode::Leaf { value: leaf, extension: self.trail.into_boxed_slice() }
        } else {
            assert!(self.child.is_some());
            assert!(self.children.is_empty());
            InputMemTrieNode::Extension {
                extension: self.trail.into_boxed_slice(),
                child: self.child.unwrap(),
            }
        };
        MemTrieNodeId::new(arena, input_node)
    }
}

impl<'a> TrieConstructor<'a> {
    pub fn new(arena: &'a mut Arena) -> Self {
        Self { arena, segments: vec![] }
    }

    fn pop_segment(&mut self) {
        let segment = self.segments.pop().unwrap();
        let node = segment.into_node(self.arena);
        let parent = self.segments.last_mut().unwrap();
        if parent.is_branch {
            parent.children.push((NibbleSlice::from_encoded(&parent.trail).0.at(0), node));
        } else {
            assert!(parent.child.is_none());
            parent.child = Some(node);
        }
    }

    pub fn add_leaf(&mut self, key: &[u8], value: FlatStateValue) {
        let mut nibbles = NibbleSlice::new(key);
        let mut i = 0;
        while i < self.segments.len() {
            // We can't be inserting a prefix into the existing path because that
            // would violate ordering.
            assert!(!nibbles.is_empty());

            let segment = &self.segments[i];
            let (extension_nibbles, _) = NibbleSlice::from_encoded(&segment.trail);
            let common_prefix_len = nibbles.common_prefix(&extension_nibbles);
            if common_prefix_len == extension_nibbles.len() {
                nibbles = nibbles.mid(common_prefix_len);
                i += 1;
                continue;
            }

            // pop off all the extra; they have no chance to be relevant to the
            // leaf we're inserting.
            while i < self.segments.len() - 1 {
                self.pop_segment();
            }

            // If we have a common prefix, split that first.
            if common_prefix_len > 0 {
                let mut segment = self.segments.pop().unwrap();
                assert!(!segment.is_branch);
                let (extension_nibbles, was_leaf) = NibbleSlice::from_encoded(&segment.trail);
                assert_eq!(was_leaf, segment.is_leaf());
                assert_eq!(was_leaf, segment.child.is_none());

                let top_segment = TrieConstructionSegment::new_extension(
                    extension_nibbles.encoded_leftmost(common_prefix_len, false).to_vec(),
                );
                segment.trail = extension_nibbles.mid(common_prefix_len).encoded(was_leaf).to_vec();
                self.segments.push(top_segment);
                self.segments.push(segment);
                nibbles = nibbles.mid(common_prefix_len);
            }

            // Now, we know that the last segment has no overlap with the leaf.
            if self.segments.last().unwrap().is_branch {
                // If it's a branch then just add another branch.
                self.segments.last_mut().unwrap().trail =
                    nibbles.encoded_leftmost(1, false).to_vec();
                nibbles = nibbles.mid(1);
                break;
            } else {
                // Otherwise we need to split the extension.
                let segment = self.segments.pop().unwrap();
                let (extension_nibbles, was_leaf) = NibbleSlice::from_encoded(&segment.trail);
                assert_eq!(was_leaf, segment.is_leaf());
                assert_eq!(was_leaf, segment.child.is_none());

                let mut top_segment = TrieConstructionSegment::new_branch(extension_nibbles.at(0));
                if extension_nibbles.len() > 1 {
                    let mut bottom_segment = TrieConstructionSegment::new_extension(
                        extension_nibbles.mid(1).encoded(was_leaf).to_vec(),
                    );
                    bottom_segment.leaf = segment.leaf;
                    bottom_segment.child = segment.child;
                    self.segments.push(top_segment);
                    self.segments.push(bottom_segment);
                    self.pop_segment();
                } else if was_leaf {
                    let mut bottom_segment = TrieConstructionSegment::new_extension(
                        extension_nibbles.mid(extension_nibbles.len()).encoded(true).to_vec(),
                    );
                    bottom_segment.leaf = segment.leaf;
                    self.segments.push(top_segment);
                    self.segments.push(bottom_segment);
                    self.pop_segment();
                } else {
                    top_segment.children.push((extension_nibbles.at(0), segment.child.unwrap()));
                    self.segments.push(top_segment);
                }
                self.segments.last_mut().unwrap().trail =
                    nibbles.encoded_leftmost(1, false).to_vec();
                nibbles = nibbles.mid(1);
                break;
            }
        }
        // When we exit the loop, either we exited because we ran out of segments
        // (in which case this leaf has the previous leaf as a prefix) or we
        // exited in the middle and we've just added a new branch.
        if !self.segments.is_empty() && self.segments.last().unwrap().is_leaf() {
            // This is the case where we ran out of segments.
            assert!(!nibbles.is_empty());
            // We need to turn the leaf node into an extension node, add a branch node
            // to store the previous leaf, and add the new leaf in.
            let segment = self.segments.pop().unwrap();
            let (extension_nibbles, was_leaf) = NibbleSlice::from_encoded(&segment.trail);
            assert!(was_leaf);
            if !extension_nibbles.is_empty() {
                // Only make an extension segment if it was a leaf with an extension.
                let top_segment = TrieConstructionSegment::new_extension(
                    extension_nibbles.encoded(false).to_vec(),
                );
                self.segments.push(top_segment);
            }
            let mut mid_segment = TrieConstructionSegment::new_branch(nibbles.at(0));
            mid_segment.leaf = segment.leaf;
            let mut bottom_segment =
                TrieConstructionSegment::new_extension(nibbles.mid(1).encoded(true).to_vec());
            bottom_segment.leaf = Some(value);
            self.segments.push(mid_segment);
            self.segments.push(bottom_segment);
        } else {
            // Otherwise we're at one branch of a branch node (or we're at root),
            // so just add the leaf.
            let mut segment =
                TrieConstructionSegment::new_extension(nibbles.encoded(true).to_vec());
            segment.leaf = Some(value);
            self.segments.push(segment);
        }
    }

    pub fn finalize(mut self) -> MemTrieNodeId {
        while self.segments.len() > 1 {
            self.pop_segment();
        }
        match self.segments.into_iter().next() {
            Some(node) => node.into_node(self.arena),
            None => MemTrieNodeId::from(usize::MAX),
        }
    }
}
