use super::arena::concurrent::{ConcurrentArena, ConcurrentArenaForThread};
use super::arena::Arena;
use super::construction::TrieConstructor;
use super::node::{InputMemTrieNode, MemTrieNodeId};
use crate::flat::FlatStorageError;
use crate::{DBCol, NibbleSlice, RawTrieNode, RawTrieNodeWithSize, Store};
use borsh::BorshDeserialize;
use near_primitives::errors::{MissingTrieValueContext, StorageError};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;
use near_primitives::types::StateRoot;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::fmt::Debug;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

pub struct PartialStateReader {
    store: Store,
    shard_uid: ShardUId,
    root: StateRoot,
    subtree_size: usize,
}

impl PartialStateReader {
    pub fn new(store: Store, shard_uid: ShardUId, root: StateRoot, subtree_size: usize) -> Self {
        Self { store, shard_uid, root, subtree_size }
    }

    pub fn make_loading_plan(&self) -> Result<PartialTrieLoadingPlan, StorageError> {
        let mut specs = Vec::new();
        let root = self.make_loading_plan_recursive(self.root, NibblePrefix::new(), &mut specs)?;
        Ok(PartialTrieLoadingPlan { root, specs })
    }

    fn make_loading_plan_recursive(
        &self,
        hash: CryptoHash,
        mut prefix: NibblePrefix,
        specs: &mut Vec<LoadingSpec>,
    ) -> Result<PartialTrieLoadingPlanNode, StorageError> {
        let mut key = [0u8; 40];
        key[0..8].copy_from_slice(&self.shard_uid.to_bytes());
        key[8..40].copy_from_slice(&hash.0);
        let node = RawTrieNodeWithSize::try_from_slice(
            &self
                .store
                .get(DBCol::State, &key)
                .map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?
                .ok_or_else(|| {
                    StorageError::MissingTrieValue(MissingTrieValueContext::TrieStorage, hash)
                })?
                .as_slice(),
        )
        .map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?;

        if node.memory_usage <= self.subtree_size as u64 {
            let spec = LoadingSpec { prefix };
            specs.push(spec);
            return Ok(PartialTrieLoadingPlanNode::Load { spec_id: specs.len() - 1 });
        }

        match node.node {
            RawTrieNode::Leaf(extension, value_ref) => {
                key[8..40].copy_from_slice(&value_ref.hash.0);
                let value = self
                    .store
                    .get(DBCol::State, &key)
                    .map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?
                    .ok_or_else(|| {
                        StorageError::MissingTrieValue(MissingTrieValueContext::TrieStorage, hash)
                    })?;
                let flat_value = FlatStateValue::on_disk(&value);
                Ok(PartialTrieLoadingPlanNode::Leaf {
                    extension: extension.into_boxed_slice(),
                    value: flat_value,
                })
            }
            RawTrieNode::BranchNoValue(children_hashes) => {
                let mut children = Vec::new();
                for i in 0..16 {
                    if let Some(child_hash) = children_hashes[i] {
                        let mut prefix = prefix.clone();
                        prefix.push(i as u8);
                        let child = self.make_loading_plan_recursive(child_hash, prefix, specs)?;
                        children.push((i as u8, Box::new(child)));
                    }
                }
                Ok(PartialTrieLoadingPlanNode::Branch { children, value: None })
            }
            RawTrieNode::BranchWithValue(value_ref, children_hashes) => {
                key[8..40].copy_from_slice(&value_ref.hash.0);
                let value = self
                    .store
                    .get(DBCol::State, &key)
                    .map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?
                    .ok_or_else(|| {
                        StorageError::MissingTrieValue(MissingTrieValueContext::TrieStorage, hash)
                    })?;
                let flat_value = FlatStateValue::on_disk(&value);

                let mut children = Vec::new();
                for i in 0..16 {
                    if let Some(child_hash) = children_hashes[i] {
                        let mut prefix = prefix.clone();
                        prefix.push(i as u8);
                        let child = self.make_loading_plan_recursive(child_hash, prefix, specs)?;
                        children.push((i as u8, Box::new(child)));
                    }
                }
                Ok(PartialTrieLoadingPlanNode::Branch { children, value: Some(flat_value) })
            }
            RawTrieNode::Extension(extension, child) => {
                let nibbles = NibbleSlice::from_encoded(&extension).0;
                prefix.append(&nibbles);
                let child = self.make_loading_plan_recursive(child, prefix, specs)?;
                Ok(PartialTrieLoadingPlanNode::Extension {
                    extension: extension.into_boxed_slice(),
                    child: Box::new(child),
                })
            }
        }
    }

    fn load(
        &self,
        spec: &LoadingSpec,
        arena: &mut ConcurrentArenaForThread,
    ) -> Result<MemTrieNodeId, StorageError> {
        let (start, end) = spec.prefix.to_iter_range(self.shard_uid);

        let mut recon = TrieConstructor::new(arena);
        for item in self.store.iter_range_raw_bytes(
            DBCol::FlatState,
            Some(&start),
            end.as_ref().map(|v| v.as_slice()),
        ) {
            let (key, value) = item.map_err(|err| {
                FlatStorageError::StorageInternalError(format!(
                    "Error iterating over FlatState: {err}"
                ))
            })?;
            let key = NibbleSlice::new(&key[8..]).mid(spec.prefix.len());
            let value = FlatStateValue::try_from_slice(&value).map_err(|err| {
                FlatStorageError::StorageInternalError(format!(
                    "invalid FlatState value format: {err}"
                ))
            })?;
            recon.add_leaf(key, value);
        }
        Ok(recon.finalize().unwrap())
    }

    fn load_in_parallel(
        &self,
        plan: PartialTrieLoadingPlan,
        name: String,
    ) -> Result<(Arena, MemTrieNodeId), StorageError> {
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

        let roots = plan
            .specs
            .into_par_iter()
            .map_init(f, |arena, spec| {
                let mut arena = arena.lock().unwrap();
                self.load(&spec, &mut arena)
            })
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let threads = std::mem::take(threads.lock().unwrap().deref_mut())
            .into_iter()
            .map(|arc| Arc::into_inner(arc).unwrap().into_inner().unwrap())
            .collect::<Vec<_>>();

        let mut arena = arena.to_single_threaded(name, threads);
        let root = plan.root.to_node(&mut arena, &roots);
        Ok((arena, root))
    }
}

#[derive(Debug)]
enum PartialTrieLoadingPlanNode {
    Branch { children: Vec<(u8, Box<PartialTrieLoadingPlanNode>)>, value: Option<FlatStateValue> },
    Extension { extension: Box<[u8]>, child: Box<PartialTrieLoadingPlanNode> },
    Leaf { extension: Box<[u8]>, value: FlatStateValue },
    Load { spec_id: usize },
}

impl PartialTrieLoadingPlanNode {
    fn to_node<'a>(self, arena: &mut Arena, roots: &[MemTrieNodeId]) -> MemTrieNodeId {
        match self {
            PartialTrieLoadingPlanNode::Branch { children, value } => {
                let mut res_children = [None; 16];
                for (nibble, child) in children {
                    res_children[nibble as usize] = Some(child.to_node(arena, roots));
                }
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
                let input = InputMemTrieNode::Leaf { extension: &extension, value: &value };
                MemTrieNodeId::new(arena, input)
            }
            PartialTrieLoadingPlanNode::Load { spec_id } => roots[spec_id],
        }
    }
}

#[derive(Debug)]
struct PartialTrieLoadingPlan {
    root: PartialTrieLoadingPlanNode,
    specs: Vec<LoadingSpec>,
}

#[derive(Debug)]
struct LoadingSpec {
    prefix: NibblePrefix,
}

#[derive(Clone)]
struct NibblePrefix {
    prefix: Vec<u8>,
    last_half: bool,
}

impl Debug for NibblePrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.last_half {
            write!(
                f,
                "{}{:x}",
                hex::encode(&self.prefix[..self.prefix.len() - 1]),
                self.prefix.last().unwrap() >> 4
            )
        } else {
            write!(f, "{}", hex::encode(&self.prefix))
        }
    }
}

impl NibblePrefix {
    pub fn new() -> Self {
        Self { prefix: Vec::new(), last_half: false }
    }

    pub fn len(&self) -> usize {
        if self.last_half {
            self.prefix.len() * 2 - 1
        } else {
            self.prefix.len() * 2
        }
    }

    pub fn push(&mut self, nibble: u8) {
        if self.last_half {
            *self.prefix.last_mut().unwrap() |= nibble;
        } else {
            self.prefix.push(nibble << 4);
        }
        self.last_half = !self.last_half;
    }

    pub fn append(&mut self, nibbles: &NibbleSlice) {
        for nibble in nibbles.iter() {
            self.push(nibble);
        }
    }

    pub fn to_iter_range(&self, shard_uid: ShardUId) -> (Vec<u8>, Option<Vec<u8>>) {
        let start = shard_uid
            .to_bytes()
            .into_iter()
            .chain(self.prefix.clone().into_iter())
            .collect::<Vec<u8>>();
        let end = increment_vec_as_num(&start, if self.last_half { 16 } else { 1 });
        (start, end)
    }
}

fn increment_vec_as_num(orig: &Vec<u8>, by: u8) -> Option<Vec<u8>> {
    let mut v = orig.clone();
    let mut carry = by;
    for i in (0..v.len()).rev() {
        let (new_val, new_carry) = v[i].overflowing_add(carry);
        v[i] = new_val;
        if new_carry {
            carry = 1;
        } else {
            carry = 0;
            break;
        }
    }
    if carry != 0 {
        None
    } else {
        Some(v)
    }
}

pub fn load_memtrie_in_parallel(
    store: Store,
    shard_uid: ShardUId,
    root: StateRoot,
    subtree_size: usize,
    name: String,
) -> Result<(Arena, MemTrieNodeId), StorageError> {
    let reader = PartialStateReader::new(store, shard_uid, root, subtree_size);
    let plan = reader.make_loading_plan()?;
    println!("Loading {} subtrees in parallel", plan.specs.len());
    reader.load_in_parallel(plan, name)
}

#[cfg(test)]
mod tests {
    use crate::trie::mem::partial_state::increment_vec_as_num;

    #[test]
    fn test_increment_vec_as_num() {
        assert_eq!(increment_vec_as_num(&vec![0, 0, 0], 1), Some(vec![0, 0, 1]));
        assert_eq!(increment_vec_as_num(&vec![0, 0, 255], 1), Some(vec![0, 1, 0]));
        assert_eq!(increment_vec_as_num(&vec![0, 255, 255], 1), Some(vec![1, 0, 0]));
        assert_eq!(increment_vec_as_num(&vec![255, 255, 254], 2), None);
    }
}
