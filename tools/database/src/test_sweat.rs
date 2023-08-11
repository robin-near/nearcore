use borsh::{BorshDeserialize, BorshSerialize};
use clap::Parser;
use near_primitives::config::{ExtCosts, ExtCostsConfig};
use near_primitives::state::FlatStateValue;
use near_primitives::trie_key::trie_key_parsers::parse_data_key_from_contract_data_key;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, Gas};
use near_store::db::{Database, RocksDB};
use near_store::flat::store_helper;
use near_store::{Store, Trie, TrieCache, TrieCachingStorage, TrieConfig, TrieUpdate};
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::flat_nodes::FlatNodesTrie;
use crate::utils::{flat_head_state_root, flush_disk_cache, open_rocksdb, sweat_shard};

#[derive(Parser)]
pub(crate) struct TestSweatCommand {
    #[arg(short, long, default_value_t = 800)]
    request_count: usize,
}

const MAX_REQUEST_COUNT: usize = 10000;

impl TestSweatCommand {
    pub(crate) fn run(&self, home: &Path) -> anyhow::Result<()> {
        if self.request_count > MAX_REQUEST_COUNT {
            panic!("max request count is {MAX_REQUEST_COUNT}, got {}", self.request_count);
        }
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
        let trie_update = TrieUpdate::new(Trie::new(
            Rc::new(storage),
            flat_head_state_root(&store, &shard_uid),
            None,
        ));
        let flat_trie = FlatNodesTrie::new(shard_uid, store.clone());
        trie_update.set_trie_cache_mode(near_primitives::types::TrieCacheMode::CachingChunk);
        let trie = trie_update.trie();
        let costs_config = ExtCostsConfig::test();
        eprintln!("Start SWEAT test with {} keys", self.request_count);
        flush_disk_cache();
        let keys: Vec<_> =
            generate_sweat_request_keys(&rocksdb).into_iter().take(self.request_count).collect();
        let nodes_before = trie.get_trie_nodes_count();
        warm_up_tries(trie, &flat_trie);
        let mut total_elapsed_trie = Duration::ZERO;
        let mut total_elapsed_flat = Duration::ZERO;
        let mut all_db_reads_elapsed_flat = Vec::new();
        let mut all_nodes_sizes_flat = Vec::new();
        let mut gas: Gas = 0;
        for key in keys.iter() {
            // read trie
            let trie_nodes_before = trie.get_trie_nodes_count();
            let start_trie = Instant::now();
            let trie_value_ref = trie.get_ref(key, near_store::KeyLookupMode::Trie).unwrap();
            total_elapsed_trie += start_trie.elapsed();
            assert!(trie_value_ref.is_some());
            let trie_nodes_count =
                trie.get_trie_nodes_count().checked_sub(&trie_nodes_before).unwrap();
            // read flat
            let start_flat = Instant::now();
            let flat_data = flat_trie.get_ref(key);
            total_elapsed_flat += start_flat.elapsed();
            all_db_reads_elapsed_flat.extend_from_slice(&flat_data.elapsed_db_reads);
            all_nodes_sizes_flat.extend_from_slice(&flat_data.nodes_sizes);
            assert_eq!(flat_data.value_ref, trie_value_ref);
            assert_eq!(flat_data.nodes_count, trie_nodes_count);
            // Update gas
            let contract_key =
                parse_data_key_from_contract_data_key(&key, &sweat_account_id()).unwrap();
            gas += costs_config.gas_cost(ExtCosts::storage_write_base)
                + costs_config.gas_cost(ExtCosts::storage_write_key_byte)
                    * (contract_key.len() as u64)
                + costs_config.gas_cost(ExtCosts::touching_trie_node) * trie_nodes_count.db_reads
                + costs_config.gas_cost(ExtCosts::read_cached_trie_node)
                    * trie_nodes_count.mem_reads;
        }
        eprintln!(
            "Read {} keys: trie elapsed {total_elapsed_trie:?}, flat elapsed {total_elapsed_flat:?}",
            keys.len(),
        );
        let nodes = trie.get_trie_nodes_count().checked_sub(&nodes_before).unwrap();
        eprintln!("Node reads: {nodes:?}, total gas: {}TGas", gas / 10e12 as u64);

        all_db_reads_elapsed_flat.sort();
        eprintln!(
            "flat db reads avg = {:?}",
            all_db_reads_elapsed_flat.iter().sum::<Duration>()
                / all_db_reads_elapsed_flat.len() as u32
        );
        let rocksdb_cache_hit_cnt =
            all_db_reads_elapsed_flat.iter().filter(|&d| *d < Duration::from_micros(100)).count();
        eprintln!(
            "flat db reads cache hit ratio {}",
            rocksdb_cache_hit_cnt as f64 / all_db_reads_elapsed_flat.len() as f64
        );
        for p in [0.5, 0.75, 0.9, 0.95, 0.99, 0.999] {
            let i = (p * all_db_reads_elapsed_flat.len() as f64) as usize;
            eprintln!("flat db reads p{p} = {:?}", all_db_reads_elapsed_flat[i]);
        }

        all_nodes_sizes_flat.sort();
        eprintln!(
            "node size avg = {:?}",
            all_nodes_sizes_flat.iter().sum::<usize>() / all_nodes_sizes_flat.len()
        );
        for p in [0.1, 0.25, 0.5, 0.75, 0.9, 0.95] {
            let i = (p * all_nodes_sizes_flat.len() as f64) as usize;
            eprintln!("node size p{p} = {:?}", all_nodes_sizes_flat[i]);
        }

        eprintln!("Finished SWEAT test");
        Ok(())
    }
}

fn warm_up_tries(trie: &Trie, flat_trie: &FlatNodesTrie) {
    eprintln!("Start tries warmup");
    let key = sweat_contract_data(vec![]);
    let trie_start = Instant::now();
    trie.get_ref(&key, near_store::KeyLookupMode::Trie).unwrap();
    let elapsed_trie = trie_start.elapsed();
    let flat_start = Instant::now();
    flat_trie.get_ref(&key);
    let elapsed_flat = flat_start.elapsed();
    eprintln!("Finished warmup: trie {elapsed_trie:?}, flat: {elapsed_flat:?}");
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
    #[derive(BorshSerialize, BorshDeserialize)]
    struct RequestKeys {
        keys: Vec<Vec<u8>>,
    }
    eprintln!("Generate contract data keys");
    if let Ok(bytes) = std::fs::read(CACHE_PATH) {
        if let Ok(req_keys) = RequestKeys::try_from_slice(&bytes) {
            if req_keys.keys.len() == MAX_REQUEST_COUNT {
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
            .take(MAX_REQUEST_COUNT)
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
