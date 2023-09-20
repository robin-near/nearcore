use std::time::Instant;

use borsh::BorshSerialize;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use crate::flat::store_helper::decode_flat_state_db_key;
use crate::trie::mem::construction::TrieConstructor;
use crate::{DBCol, Store};

use super::node::MemTrieNodeId;
use super::MemTries;

pub fn load_trie_from_flat_state(
    store: &Store,
    shard_uid: ShardUId,
    state_root: CryptoHash,
) -> anyhow::Result<MemTries> {
    let mut tries = MemTries::new(64 * 1024 * 1024 * 1024);

    tries.construct_root(state_root, |arena| -> anyhow::Result<MemTrieNodeId> {
        println!("Loading trie from flat state...");
        let load_start = Instant::now();
        let mut recon = TrieConstructor::new(arena);
        let mut loaded = 0;
        for item in store
            .iter_prefix_ser::<FlatStateValue>(DBCol::FlatState, &shard_uid.try_to_vec().unwrap())
        {
            let (key, value) = item?;
            let (_, key) = decode_flat_state_db_key(&key)?;
            recon.add_leaf(&key, value);
            loaded += 1;
            if loaded % 1000000 == 0 {
                println!(
                    "[{:?}] Loaded {} keys, current key: {}",
                    load_start.elapsed(),
                    loaded,
                    hex::encode(&key)
                );
            }
        }
        let root_id = recon.finalize();

        println!(
            "[{:?}] Loaded {} keys; computing hash and memory usage...",
            load_start.elapsed(),
            loaded
        );
        let mut subtrees = Vec::new();
        root_id.as_ptr_mut(arena.memory_mut()).take_small_subtrees(1024 * 1024, &mut subtrees);
        println!(
            "[{:?}] Going to compute hash and memory for {} subtrees in parallel...",
            load_start.elapsed(),
            subtrees.len()
        );
        subtrees.into_par_iter().for_each(|mut subtree| {
            subtree.compute_hash_recursively();
        });
        println!(
            "[{:?}] Done computing hash and memory usage for subtrees; now computing root hash",
            load_start.elapsed()
        );
        root_id.as_ptr_mut(arena.memory_mut()).compute_hash_recursively();

        let root = root_id.as_ptr(arena.memory());
        if root.view().node_hash() != state_root {
            panic!(
                "[{:?}] State root mismatch: expected {:?}, actual {:?}",
                load_start.elapsed(),
                state_root,
                root.view().node_hash()
            );
        } else {
            println!("[{:?}] Done loading trie from flat state", load_start.elapsed());
        }
        Ok(root.id())
    })?;
    Ok(tries)
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::state::FlatStateValue;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use crate::test_utils::{
        create_tries, simplify_changes, test_populate_flat_storage, test_populate_trie,
    };
    use crate::trie::mem::arena::Arena;
    use crate::trie::mem::construction::TrieConstructor;
    use crate::trie::mem::loading::load_trie_from_flat_state;
    use crate::trie::mem::lookup::MemTrieLookup;
    use crate::{KeyLookupMode, NibbleSlice, Trie, TrieUpdate};

    #[test]
    fn test_basic_reconstruction() {
        let mut arena = Arena::new(64 * 1024 * 1024 * 1024);
        let mut rec = TrieConstructor::new(&mut arena);
        rec.add_leaf(b"aaaaa", FlatStateValue::Inlined(b"a".to_vec()));
        rec.add_leaf(b"aaaab", FlatStateValue::Inlined(b"b".to_vec()));
        rec.add_leaf(b"ab", FlatStateValue::Inlined(b"c".to_vec()));
        rec.add_leaf(b"abffff", FlatStateValue::Inlined(b"c".to_vec()));
        rec.finalize();
    }

    fn check(keys: Vec<Vec<u8>>) {
        let shard_tries = create_tries();
        let shard_uid = ShardUId::single_shard();
        let changes = keys.iter().map(|key| (key.to_vec(), Some(key.to_vec()))).collect::<Vec<_>>();
        let changes = simplify_changes(&changes);
        test_populate_flat_storage(
            &shard_tries,
            shard_uid,
            &CryptoHash::default(),
            &CryptoHash::default(),
            &changes,
        );
        let state_root = test_populate_trie(&shard_tries, &Trie::EMPTY_ROOT, shard_uid, changes);

        eprintln!("Trie and flat storage populated");
        let in_memory_trie =
            load_trie_from_flat_state(&shard_tries.get_store(), shard_uid, state_root).unwrap();
        eprintln!("In memory trie loaded");

        let trie_update = TrieUpdate::new(shard_tries.get_trie_for_shard(shard_uid, state_root));
        trie_update.set_trie_cache_mode(near_primitives::types::TrieCacheMode::CachingChunk);
        let trie = trie_update.trie();
        let lookup = MemTrieLookup::new(in_memory_trie.get_root(&state_root).unwrap());
        for key in keys.iter() {
            let actual_value_ref = lookup.get_ref(key).map(|v| v.to_value_ref());
            let expected_value_ref = trie.get_ref(key, KeyLookupMode::Trie).unwrap();
            assert_eq!(actual_value_ref, expected_value_ref, "{:?}", NibbleSlice::new(key));
            assert_eq!(lookup.get_nodes_count(), trie.get_trie_nodes_count());
        }
    }

    fn check_random(max_key_len: usize, max_keys_count: usize, test_count: usize) {
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..test_count {
            let key_cnt = rng.gen_range(1..=max_keys_count);
            let mut keys = Vec::new();
            for _ in 0..key_cnt {
                let mut key = Vec::new();
                let key_len = rng.gen_range(0..=max_key_len);
                for _ in 0..key_len {
                    let byte: u8 = rng.gen();
                    key.push(byte);
                }
                keys.push(key);
            }
            check(keys);
        }
    }

    #[test]
    fn test_memtrie_basic() {
        check(vec![vec![0, 1], vec![1, 0]]);
    }

    #[test]
    fn test_memtrie_rand_small() {
        check_random(3, 20, 10);
    }

    #[test]
    fn test_memtrie_rand_many_keys() {
        check_random(5, 1000, 10);
    }

    #[test]
    fn test_memtrie_rand_long_keys() {
        check_random(20, 100, 10);
    }

    #[test]
    fn test_memtrie_rand_long_long_keys() {
        check_random(1000, 1000, 1);
    }

    #[test]
    fn test_memtrie_rand_large_data() {
        check_random(32, 100000, 10);
    }
}
