use super::arena::concurrent::{ConcurrentArena, ConcurrentArenaForThread};
use super::arena::Arena;
use super::construction::TrieConstructor;
use super::loading::MemEfficientFlatStateValue;
use super::node::{InputMemTrieNode, MemTrieNodeId};
use crate::NibbleSlice;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::fmt::{Debug, Formatter};
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

type Entry = (Box<[u8]>, MemEfficientFlatStateValue);

struct LoadingRanges {
    ranges: Vec<Vec<Entry>>,
    entries_before_range: Vec<usize>,
    max_entries_per_split: usize,
}

impl LoadingRanges {
    fn from_ranges(ranges: Vec<Vec<Entry>>, entries_eligible_for_split: usize) -> Self {
        assert!(!ranges.is_empty());
        let mut entries_before_range = Vec::with_capacity(ranges.len());
        let mut cum_len_sum = 0;
        for range in &ranges {
            assert!(!range.is_empty());
            entries_before_range.push(cum_len_sum);
            cum_len_sum += range.len();
        }
        Self { ranges, entries_before_range, max_entries_per_split: entries_eligible_for_split }
    }

    fn begin(&self) -> TwoLevelIndex {
        TwoLevelIndex { range_index: 0, entry_index: 0 }
    }

    fn end(&self) -> TwoLevelIndex {
        TwoLevelIndex {
            range_index: self.ranges.len() - 1,
            entry_index: self.ranges.last().unwrap().len() - 1,
        }
    }

    fn next_index(&self, index: TwoLevelIndex) -> Option<TwoLevelIndex> {
        let range = &self.ranges[index.range_index];
        if index.entry_index + 1 < range.len() {
            Some(TwoLevelIndex {
                range_index: index.range_index,
                entry_index: index.entry_index + 1,
            })
        } else if index.range_index + 1 < self.ranges.len() {
            Some(TwoLevelIndex { range_index: index.range_index + 1, entry_index: 0 })
        } else {
            None
        }
    }

    fn prev_index(&self, index: TwoLevelIndex) -> Option<TwoLevelIndex> {
        if index.entry_index > 0 {
            Some(TwoLevelIndex {
                range_index: index.range_index,
                entry_index: index.entry_index - 1,
            })
        } else if index.range_index > 0 {
            let prev_range = &self.ranges[index.range_index - 1];
            Some(TwoLevelIndex {
                range_index: index.range_index - 1,
                entry_index: prev_range.len() - 1,
            })
        } else {
            None
        }
    }

    fn key_at(&self, index: TwoLevelIndex) -> &[u8] {
        &self.ranges[index.range_index][index.entry_index].0
    }
}

fn nibble_at(key: &[u8], nibble_index: usize) -> u8 {
    let byte_index = nibble_index / 2;
    if nibble_index % 2 == 0 {
        key[byte_index] >> 4
    } else {
        key[byte_index] & 0xf
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
struct TwoLevelIndex {
    range_index: usize,
    entry_index: usize,
}

#[derive(Clone, Debug)]
struct ConstructionPrefix {
    begin: TwoLevelIndex,
    end: TwoLevelIndex,
    // The length of the prefix. It can be obtained by looking at the range.
    common_prefix_len: usize,
}

#[derive(Debug)]
enum ConstructionPartialTrie {
    Branch { children: Vec<(u8, Box<ConstructionPartialTrie>)>, value: Option<TwoLevelIndex> },
    Extension { extension: Box<[u8]>, child: Box<ConstructionPartialTrie> },
    Leaf { extension: Box<[u8]>, value: TwoLevelIndex },
    Deferred { prefix: ConstructionPrefix },
}

impl LoadingRanges {
    fn find_split(
        &self,
        begin: TwoLevelIndex,
        end: TwoLevelIndex,
        nibble_index: usize,
        nibble: u8,
    ) -> Option<TwoLevelIndex> {
        if nibble_at(self.key_at(end), nibble_index) < nibble {
            return None;
        }

        let mut left = begin;
        let mut right = end;
        while left.range_index < right.range_index {
            let mid = (left.range_index + right.range_index) / 2;
            let range = &self.ranges[mid];
            let key = &range.last().unwrap().0;
            let entry_nibble = nibble_at(key, nibble_index);
            if entry_nibble < nibble {
                // this whole range is before the split.
                left.range_index = mid + 1;
                left.entry_index = 0;
            } else {
                // The last of this range is on or after the split.
                right.range_index = mid;
                right.entry_index = range.len() - 1;
            }
        }

        let range = &self.ranges[left.range_index];
        while left.entry_index < right.entry_index {
            let mid = (left.entry_index + right.entry_index) / 2;
            let entry_key = &range[mid].0;
            let entry_nibble = nibble_at(entry_key, nibble_index);
            if entry_nibble < nibble {
                left.entry_index = mid + 1;
            } else {
                right.entry_index = mid;
            }
        }
        Some(left)
    }

    fn num_entries_before(&self, index: TwoLevelIndex) -> usize {
        self.entries_before_range[index.range_index] + index.entry_index
    }

    fn num_entries_in_prefix(&self, range: &ConstructionPrefix) -> usize {
        let begin = self.num_entries_before(range.begin);
        let end = self.num_entries_before(range.end);
        end - begin + 1
    }

    fn split_by_nibble_recursive(
        &self,
        begin: TwoLevelIndex,
        end: TwoLevelIndex,
        nibble_index: usize,
        result: &mut Vec<(u8, Box<ConstructionPartialTrie>)>,
        nibble: u8,
        mask: u8,
    ) {
        if mask == 0 {
            result.push((
                nibble,
                Box::new(self.split_prefix(ConstructionPrefix {
                    begin,
                    end,
                    common_prefix_len: nibble_index + 1,
                })),
            ));
            return;
        }
        let split = self.find_split(begin, end, nibble_index, nibble + mask);
        let next_mask = mask >> 1;
        match split {
            Some(split) => {
                if split == begin {
                    self.split_by_nibble_recursive(
                        begin,
                        end,
                        nibble_index,
                        result,
                        nibble + mask,
                        next_mask,
                    );
                } else {
                    self.split_by_nibble_recursive(
                        begin,
                        self.prev_index(split).unwrap(),
                        nibble_index,
                        result,
                        nibble,
                        next_mask,
                    );
                    self.split_by_nibble_recursive(
                        split,
                        end,
                        nibble_index,
                        result,
                        nibble + mask,
                        next_mask,
                    );
                }
            }
            None => {
                self.split_by_nibble_recursive(begin, end, nibble_index, result, nibble, next_mask);
            }
        }
    }

    fn split_prefix(&self, mut prefix: ConstructionPrefix) -> ConstructionPartialTrie {
        if self.num_entries_in_prefix(&prefix) <= self.max_entries_per_split {
            return ConstructionPartialTrie::Deferred { prefix };
        }
        let begin_key = self.key_at(prefix.begin);
        if prefix.begin == prefix.end {
            let mut extension = Vec::new();
            NibbleSlice::new(begin_key)
                .mid(prefix.common_prefix_len)
                .encode_to(true, &mut extension);
            return ConstructionPartialTrie::Leaf {
                extension: extension.into_boxed_slice(),
                value: prefix.begin,
            };
        }

        if begin_key.len() * 2 == prefix.common_prefix_len {
            let next_key = self.next_index(prefix.begin).unwrap(); // must succeed because begin != end
            let mut split = Vec::new();
            self.split_by_nibble_recursive(
                next_key,
                prefix.end,
                prefix.common_prefix_len,
                &mut split,
                0,
                0b1000,
            );
            return ConstructionPartialTrie::Branch { children: split, value: Some(prefix.begin) };
        }

        let extension_begin = prefix.common_prefix_len;
        let begin_key = self.key_at(prefix.begin);
        let end_key = self.key_at(prefix.end);
        while begin_key.len() * 2 > prefix.common_prefix_len
            && nibble_at(begin_key, prefix.common_prefix_len)
                == nibble_at(end_key, prefix.common_prefix_len)
        {
            prefix.common_prefix_len += 1;
        }
        if prefix.common_prefix_len != extension_begin {
            let mut extension = Vec::new();
            NibbleSlice::new(begin_key).mid(extension_begin).encode_leftmost_to(
                prefix.common_prefix_len - extension_begin,
                false,
                &mut extension,
            );
            return ConstructionPartialTrie::Extension {
                extension: extension.into_boxed_slice(),
                child: Box::new(self.split_prefix(prefix)),
            };
        }

        let mut split = Vec::new();
        self.split_by_nibble_recursive(
            prefix.begin,
            prefix.end,
            prefix.common_prefix_len,
            &mut split,
            0,
            0b1000,
        );
        ConstructionPartialTrie::Branch { children: split, value: None }
    }

    fn split_root(&self) -> ConstructionPartialTrie {
        self.split_prefix(ConstructionPrefix {
            begin: self.begin(),
            end: self.end(),
            common_prefix_len: 0,
        })
    }

    fn to_loading_plan_node<'a>(
        ranges: &mut TwoLevelMutRange<'a>,
        plan: ConstructionPartialTrie,
        specs: &mut Vec<LoadingSpec<'a>>,
    ) -> PartialTrieLoadingPlanNode {
        match plan {
            ConstructionPartialTrie::Branch { children, value } => {
                // Must query value first before children!
                let value = value.map(|x| ranges.value(x));
                let children = children
                    .into_iter()
                    .map(|(nibble, child)| {
                        (nibble, Box::new(Self::to_loading_plan_node(ranges, *child, specs)))
                    })
                    .collect();
                PartialTrieLoadingPlanNode::Branch { children, value }
            }
            ConstructionPartialTrie::Extension { extension, child } => {
                PartialTrieLoadingPlanNode::Extension {
                    extension,
                    child: Box::new(Self::to_loading_plan_node(ranges, *child, specs)),
                }
            }
            ConstructionPartialTrie::Leaf { extension, value } => {
                PartialTrieLoadingPlanNode::Leaf { extension, value: ranges.value(value) }
            }
            ConstructionPartialTrie::Deferred { prefix } => {
                let spec = ranges.take_entries_for_range(
                    prefix.begin,
                    prefix.end,
                    prefix.common_prefix_len,
                );
                specs.push(spec);
                PartialTrieLoadingPlanNode::Load { spec_id: specs.len() - 1 }
            }
        }
    }

    fn to_loading_plan(&mut self, plan: ConstructionPartialTrie) -> PartialTrieLoadingPlan {
        let mut ranges = TwoLevelMutRange::new(&mut self.ranges);
        let mut specs = Vec::new();
        let root = Self::to_loading_plan_node(&mut ranges, plan, &mut specs);
        PartialTrieLoadingPlan { root, specs }
    }

    fn load_in_parallel(&mut self, name: String) -> (Arena, MemTrieNodeId) {
        let plan = self.split_root();
        // println!("{:?}", plan);
        let plan = self.to_loading_plan(plan);
        // println!("{:?}", plan);
        plan.load_in_parallel(name)
    }
}

pub fn load_memtrie_in_parallel(
    ranges: Vec<Vec<Entry>>,
    entries_eligible_for_split: usize,
    shard_uid: ShardUId,
) -> (Arena, MemTrieNodeId) {
    let mut ranges = LoadingRanges::from_ranges(ranges, entries_eligible_for_split);
    ranges.load_in_parallel(shard_uid.to_string())
}

struct TwoLevelMutRange<'a> {
    current: TwoLevelIndex,
    partial: &'a mut [Entry],
    full: &'a mut [Vec<Entry>],
}

impl<'a> TwoLevelMutRange<'a> {
    fn new(ranges: &'a mut [Vec<Entry>]) -> Self {
        Self {
            current: TwoLevelIndex { range_index: 0, entry_index: 0 },
            partial: &mut [],
            full: ranges,
        }
    }

    fn take_entries_in_range(
        &mut self,
        range: usize,
        from: usize,
        to: Option<usize>,
    ) -> &'a mut [Entry] {
        // println!(
        //     "take_entries_in_range, current = {:?}, range = {}, from = {:?}, to = {:?}",
        //     self.current, range, from, to
        // );
        while range > self.current.range_index {
            self.advance_to_next_range();
        }
        self.ensure_current_range_is_partial();

        let partial = std::mem::replace(&mut self.partial, &mut []);
        let (discard, partial) = partial.split_at_mut(from - self.current.entry_index);
        let (desired, rest) = match to {
            Some(to) => partial.split_at_mut(to - from + 1),
            None => (partial, &mut [] as &mut [_]),
        };
        self.current.entry_index += discard.len() + desired.len();
        self.partial = rest;
        if self.partial.is_empty() {
            self.current.range_index += 1;
            self.current.entry_index = 0;
        }
        desired
    }

    fn advance_to_next_range(&mut self) {
        // println!("advance_to_next_stage, current = {:?}", self.current);
        if self.partial.is_empty() {
            assert_eq!(self.current.entry_index, 0);
            let (_, full) = std::mem::replace(&mut self.full, &mut []).split_first_mut().unwrap();
            self.full = full;
        } else {
            self.partial = &mut [];
        }
        self.current = TwoLevelIndex { range_index: self.current.range_index + 1, entry_index: 0 };
    }

    fn ensure_current_range_is_partial(&mut self) {
        if self.current.entry_index > 0 {
            assert!(!self.partial.is_empty());
            return;
        }
        assert!(!self.full.is_empty(), "Ran out of ranges, current pos: {:?}", self.current);
        let (partial, full) = std::mem::replace(&mut self.full, &mut []).split_first_mut().unwrap();
        self.partial = partial.as_mut_slice();
        self.full = full;
    }

    fn take_full_range(&mut self, range: usize) -> Vec<Entry> {
        while range > self.current.range_index {
            self.advance_to_next_range();
        }
        assert!(
            self.partial.is_empty(),
            "Cannot take full range when the range is partially available"
        );
        let (full, rest) = std::mem::replace(&mut self.full, &mut []).split_first_mut().unwrap();
        self.full = rest;
        self.current = TwoLevelIndex { range_index: range + 1, entry_index: 0 };
        return std::mem::take(full);
    }

    fn value(&mut self, index: TwoLevelIndex) -> MemEfficientFlatStateValue {
        assert!(index >= self.current, "Cannot take value before the current position");
        let partial = self.take_entries_in_range(
            index.range_index,
            index.entry_index,
            Some(index.entry_index),
        );
        return std::mem::take(&mut partial[0]).1;
    }

    fn take_entries_for_range(
        &mut self,
        begin: TwoLevelIndex,
        end: TwoLevelIndex,
        prefix_len: usize,
    ) -> LoadingSpec<'a> {
        assert!(
            begin >= self.current,
            "Cannot take entries before the current position; requested {:?}, current {:?}",
            begin,
            self.current
        );

        if begin.range_index == end.range_index
            && !(begin.entry_index == 0
                && self.full[end.range_index - self.current.range_index].len()
                    == end.entry_index + 1)
        {
            return LoadingSpec {
                prefix_len,
                left_partial: self.take_entries_in_range(
                    begin.range_index,
                    begin.entry_index,
                    Some(end.entry_index),
                ),
                full: vec![],
                right_partial: &mut [],
            };
        }

        let (left_partial, first_full_range) = if begin.entry_index == 0 {
            (&mut [] as &mut [_], begin.range_index)
        } else {
            let left_partial =
                self.take_entries_in_range(begin.range_index, begin.entry_index, None);
            (left_partial, begin.range_index + 1)
        };
        let last_full_range =
            if self.full[end.range_index - self.current.range_index].len() == end.entry_index + 1 {
                end.range_index
            } else {
                end.range_index - 1
            };
        let mut full_ranges = Vec::new();
        for i in first_full_range..=last_full_range {
            full_ranges.push(self.take_full_range(i));
        }
        let right_partial = if end.range_index == last_full_range {
            &mut []
        } else {
            self.take_entries_in_range(end.range_index, 0, Some(end.entry_index))
        };
        LoadingSpec { prefix_len, left_partial, full: full_ranges, right_partial }
    }
}

enum PartialTrieLoadingPlanNode {
    Branch {
        children: Vec<(u8, Box<PartialTrieLoadingPlanNode>)>,
        value: Option<MemEfficientFlatStateValue>,
    },
    Extension {
        extension: Box<[u8]>,
        child: Box<PartialTrieLoadingPlanNode>,
    },
    Leaf {
        extension: Box<[u8]>,
        value: MemEfficientFlatStateValue,
    },
    Load {
        spec_id: usize,
    },
}

impl Debug for PartialTrieLoadingPlanNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartialTrieLoadingPlanNode::Branch { children, value } => f
                .debug_struct("Branch")
                .field("children", children)
                .field("value exists", &value.is_some())
                .finish(),
            PartialTrieLoadingPlanNode::Extension { extension, child } => f
                .debug_struct("Extension")
                .field("extension", &hex::encode(extension))
                .field("child", child)
                .finish(),
            PartialTrieLoadingPlanNode::Leaf { extension, .. } => {
                f.debug_struct("Leaf").field("extension", extension).finish()
            }
            PartialTrieLoadingPlanNode::Load { spec_id } => {
                f.debug_struct("Load").field("spec_id", spec_id).finish()
            }
        }
    }
}

impl PartialTrieLoadingPlanNode {
    fn to_node<'a>(self, arena: &mut Arena, roots: &[MemTrieNodeId]) -> MemTrieNodeId {
        match self {
            PartialTrieLoadingPlanNode::Branch { children, value } => {
                let mut res_children = [None; 16];
                for (nibble, child) in children {
                    res_children[nibble as usize] = Some(child.to_node(arena, roots));
                }
                let value: Option<FlatStateValue> = value.map(|value| value.into());
                let input = match &value {
                    Some(value) => {
                        InputMemTrieNode::BranchWithValue { children: res_children, value }
                    }
                    None => InputMemTrieNode::Branch { children: res_children },
                };
                MemTrieNodeId::new(arena, input)
            }
            PartialTrieLoadingPlanNode::Extension { extension, child } => {
                let child = child.to_node(arena, roots);
                let input = InputMemTrieNode::Extension { extension: &extension, child };
                MemTrieNodeId::new(arena, input)
            }
            PartialTrieLoadingPlanNode::Leaf { extension, value } => {
                let value: FlatStateValue = value.into();
                let input = InputMemTrieNode::Leaf { extension: &extension, value: &value };
                MemTrieNodeId::new(arena, input)
            }
            PartialTrieLoadingPlanNode::Load { spec_id } => roots[spec_id],
        }
    }
}

#[derive(Debug)]
struct PartialTrieLoadingPlan<'a> {
    root: PartialTrieLoadingPlanNode,
    specs: Vec<LoadingSpec<'a>>,
}

struct LoadingSpec<'a> {
    prefix_len: usize,
    left_partial: &'a mut [Entry],
    full: Vec<Vec<Entry>>,
    right_partial: &'a mut [Entry],
}

impl<'a> Debug for LoadingSpec<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadingSpec")
            .field("prefix_len", &self.prefix_len)
            .field("left_partial len", &self.left_partial.len())
            .field("full len", &self.full.len())
            .field("right_partial len", &self.right_partial.len())
            .finish()
    }
}

impl<'a> LoadingSpec<'a> {
    fn for_each(self, mut f: impl FnMut(NibbleSlice<'_>, MemEfficientFlatStateValue)) {
        let mut f = move |key: Box<[u8]>, value: MemEfficientFlatStateValue| {
            f(NibbleSlice::new(&key).mid(self.prefix_len), value);
        };
        for entry in self.left_partial {
            let (key, value) = std::mem::take(entry);
            f(key, value);
        }
        for range in self.full {
            for (key, value) in range {
                f(key, value);
            }
        }
        for entry in self.right_partial {
            let (key, value) = std::mem::take(entry);
            f(key, value);
        }
    }
}

impl<'a> PartialTrieLoadingPlan<'a> {
    fn load_in_parallel(self, name: String) -> (Arena, MemTrieNodeId) {
        let arena = ConcurrentArena::new();
        let threads: Arc<Mutex<Vec<Arc<Mutex<ConcurrentArenaForThread>>>>> =
            Arc::new(Mutex::new(Vec::new()));

        let f = {
            let arena = arena.clone();
            let threads = threads.clone();
            move || {
                let arena = arena.for_thread();
                let arena = Arc::new(Mutex::new(arena));
                threads.lock().unwrap().push(arena.clone());
                arena
            }
        };

        let roots = self
            .specs
            .into_par_iter()
            .map_init(f, |arena, spec| {
                let mut arena = arena.lock().unwrap();
                let mut con = TrieConstructor::new(arena.deref_mut());
                spec.for_each(|key, value| {
                    con.add_leaf(key, value.into());
                });
                con.finalize().unwrap()
            })
            .collect::<Vec<_>>();

        let threads = std::mem::take(threads.lock().unwrap().deref_mut())
            .into_iter()
            .map(|arc| Arc::into_inner(arc).unwrap().into_inner().unwrap())
            .collect::<Vec<_>>();

        let mut arena = arena.to_single_threaded(name, threads);
        let root = self.root.to_node(&mut arena, &roots);
        (arena, root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nibbles(hex: &str) -> Vec<u8> {
        if hex == "_" {
            return vec![];
        }
        assert!(hex.len() % 2 == 0);
        hex::decode(hex).unwrap()
    }

    fn entries(hexes: &str) -> Vec<(Box<[u8]>, MemEfficientFlatStateValue)> {
        hexes
            .split_whitespace()
            .map(|x| {
                (nibbles(x).into_boxed_slice(), MemEfficientFlatStateValue::Inlined([].into()))
            })
            .collect()
    }

    #[test]
    fn test_split() {
        let ranges = vec![
            entries("00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f"),
            entries("10 11 12 13 17 1e"),
            entries("2223 4444 5555 5556"),
        ];
        let loading_ranges = LoadingRanges::from_ranges(ranges, 0);
        let root = loading_ranges.split_root();
        println!("{:?}", root);
    }
}
