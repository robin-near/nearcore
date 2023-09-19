use clap::Parser;
use indicatif::ProgressIterator;
use near_primitives::borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::config::ExtCostsConfig;
use near_primitives::state::FlatStateValue;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::AccountId;
use near_store::db::{Database, RocksDB};
use near_store::flat::store_helper;
use near_store::trie::mem::loading::load_trie_from_flat_state;
use near_store::trie::mem::lookup::MemTrieLookup;
use near_store::{Store, Trie, TrieCache, TrieCachingStorage, TrieConfig, TrieUpdate};
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::utils::{flat_head_state_root, flush_disk_cache, open_rocksdb, sweat_shard};

#[derive(Parser)]
pub(crate) struct TestSweatCommand {
    #[arg(short, long, default_value_t = 800)]
    request_count: usize,

    #[arg(short, long, default_value_t = 100)]
    warmup_count: usize,
}

const MAX_REQUEST_COUNT: usize = 10000;

impl TestSweatCommand {
    pub(crate) fn run(&self, home: &Path) -> anyhow::Result<()> {
        if self.request_count > MAX_REQUEST_COUNT || self.warmup_count > MAX_REQUEST_COUNT {
            panic!("max request count is {MAX_REQUEST_COUNT}");
        }
        eprintln!(
            "Start SWEAT: {} test keys, {} warmup keys",
            self.request_count, self.warmup_count
        );

        let near_config = nearcore::config::load_config(
            &home,
            near_chain_configs::GenesisValidationMode::UnsafeFast,
        )?;
        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadOnly)?);
        let store = near_store::NodeStorage::new(rocksdb.clone()).get_hot_store();
        let shard_uid = sweat_shard();
        let shard_cache = TrieCache::new(
            &TrieConfig::from_store_config(&near_config.config.store),
            shard_uid,
            false,
        );
        let storage = TrieCachingStorage::new(store.clone(), shard_cache, shard_uid, false, None);
        let state_root = flat_head_state_root(&store, &shard_uid);
        let trie_update = TrieUpdate::new(Trie::new(Rc::new(storage), state_root, None));
        let in_memory_trie = load_trie_from_flat_state(&store, shard_uid, state_root)?;
        let memtrie_lookup = MemTrieLookup::new(in_memory_trie.get_root(&state_root).unwrap());

        trie_update.set_trie_cache_mode(near_primitives::types::TrieCacheMode::CachingChunk);
        let trie = trie_update.trie();
        let costs_config = ExtCostsConfig::test();
        flush_disk_cache();
        let all_keys = generate_sweat_request_keys(&rocksdb);
        let (request_keys, rest_keys) = all_keys.split_at(self.request_count);
        warm_up_tries(trie, &memtrie_lookup, rest_keys.split_at(self.warmup_count).0);
        let mut total_elapsed_trie = Duration::ZERO;
        let mut total_elapsed_mem = Duration::ZERO;
        eprintln!("Executing get_ref for {} keys", request_keys.len());
        let before_counts = memtrie_lookup.get_nodes_count();
        for key in request_keys.iter().progress() {
            // read trie
            let trie_nodes_before = trie.get_trie_nodes_count();
            let start_trie = Instant::now();
            let trie_value_ref = trie.get_ref(key, near_store::KeyLookupMode::Trie).unwrap();
            total_elapsed_trie += start_trie.elapsed();
            assert!(trie_value_ref.is_some());
            let trie_nodes_count =
                trie.get_trie_nodes_count().checked_sub(&trie_nodes_before).unwrap();
            let start_in_memory = Instant::now();
            let in_memory_data = memtrie_lookup.get_ref(key);
            total_elapsed_mem += start_in_memory.elapsed();
            assert_eq!(in_memory_data.map(|f| f.to_value_ref()), trie_value_ref);
        }
        eprintln!("Elapsed trie: {total_elapsed_trie:?}");
        eprintln!(
            "Elapsed in-memory: {total_elapsed_mem:?} ({:.2}x)",
            total_elapsed_trie.as_secs_f64() / total_elapsed_mem.as_secs_f64()
        );
        let after_counts = memtrie_lookup.get_nodes_count();
        eprintln!(
            "Trie nodes: mem: {}, db: {}",
            after_counts.mem_reads - before_counts.mem_reads,
            after_counts.db_reads - before_counts.db_reads
        );
        eprintln!("Finished SWEAT test");
        Ok(())
    }
}

fn warm_up_tries(trie: &Trie, memtrie: &MemTrieLookup, keys: &[Vec<u8>]) {
    eprintln!("Start tries warmup with {} keys", keys.len());
    let trie_start = Instant::now();
    for key in keys {
        trie.get_ref(&key, near_store::KeyLookupMode::Trie).unwrap();
    }
    eprintln!("Elapsed trie: {:?}", trie_start.elapsed());
    eprintln!("Start memtrie warmup with {} keys", keys.len());
    let trie_start = Instant::now();
    for key in keys {
        memtrie.get_ref(&key).unwrap();
    }
    eprintln!("Elapsed memtrie: {:?}", trie_start.elapsed());
}

#[allow(dead_code)]
fn assert_sweat_account(store: &Store, trie: &Trie) {
    eprintln!("Sanity check Trie read");
    let sweat_account_key = TrieKey::Account { account_id: sweat_account_id() }.to_vec();
    let fs_sweat_account =
        store_helper::get_flat_state_value(&store, sweat_shard(), &sweat_account_key).unwrap().map(
            |fs_value| match fs_value {
                FlatStateValue::Inlined(bytes) => bytes,
                FlatStateValue::Ref(_) => panic!("expected inlined value"),
            },
        );
    let trie_sweat_account = trie.get(&sweat_account_key).unwrap();
    assert_eq!(fs_sweat_account, trie_sweat_account);
}

fn generate_sweat_request_keys(rocksdb: &RocksDB) -> Vec<Vec<u8>> {
    const CACHE_PATH: &str = "/tmp/sweat_request_keys";
    const READ_COUNT: usize = 2 * MAX_REQUEST_COUNT;
    #[derive(BorshSerialize, BorshDeserialize)]
    struct RequestKeys {
        keys: Vec<Vec<u8>>,
    }
    eprintln!("Generate contract data keys");
    if let Ok(bytes) = std::fs::read(CACHE_PATH) {
        if let Ok(req_keys) = RequestKeys::try_from_slice(&bytes) {
            if req_keys.keys.len() == READ_COUNT {
                eprintln!("Read keys from the fs cache");
                return req_keys.keys;
            }
        }
    }
    eprintln!("Reading keys from flat storage");
    let mut keys = Vec::new();
    let empty_key_data = sweat_contract_data(vec![]);
    for entry in rocksdb.iter_prefix(
        near_store::DBCol::FlatState,
        &store_helper::encode_flat_state_db_key(sweat_shard(), &empty_key_data),
    ) {
        let key = entry.unwrap().0;
        keys.push(key);
    }
    eprintln!("Read total {} sweat keys", keys.len());
    keys.shuffle(&mut rand::rngs::StdRng::seed_from_u64(42));
    let req_keys = RequestKeys {
        keys: keys
            .iter()
            .take(READ_COUNT)
            .map(|key| store_helper::decode_flat_state_db_key(&key).unwrap().1)
            .collect(),
    };
    std::fs::write(CACHE_PATH, req_keys.try_to_vec().unwrap()).unwrap();
    req_keys.keys
}

fn sweat_contract_data(key: Vec<u8>) -> Vec<u8> {
    let contract_data = TrieKey::ContractData { account_id: sweat_account_id(), key };
    contract_data.to_vec()
}

fn sweat_account_id() -> AccountId {
    "token.sweat".parse().unwrap()
}
