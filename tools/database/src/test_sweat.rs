use borsh::{BorshDeserialize, BorshSerialize};
use clap::Parser;
use near_primitives::config::{ExtCosts, ExtCostsConfig};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{get_block_shard_uid, ShardLayout};
use near_primitives::state::FlatStateValue;
use near_primitives::trie_key::trie_key_parsers::parse_data_key_from_contract_data_key;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, Gas};
use near_store::db::{Database, RocksDB};
use near_store::flat::store_helper;
use near_store::{
    DBCol, ShardUId, Store, Trie, TrieCache, TrieCachingStorage, TrieConfig, TrieUpdate,
};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use crate::utils::open_rocksdb;

#[derive(Parser)]
pub(crate) struct TestSweatCommand {}

const REQUEST_COUNT: usize = 800;

impl TestSweatCommand {
    pub(crate) fn run(&self, home: &Path) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(
            &home,
            near_chain_configs::GenesisValidationMode::UnsafeFast,
        )?;
        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadOnly)?);
        let store = near_store::NodeStorage::new(rocksdb.clone()).get_hot_store();
        let shard_cache = TrieCache::new(
            &TrieConfig::from_store_config(&near_config.config.store),
            sweat_shard(),
            false,
        );
        let storage =
            TrieCachingStorage::new(store.clone(), shard_cache, sweat_shard(), false, None);
        let trie_update = TrieUpdate::new(Trie::new(Rc::new(storage), state_root(&store), None));
        trie_update.set_trie_cache_mode(near_primitives::types::TrieCacheMode::CachingChunk);
        let trie = trie_update.trie();
        let costs_config = ExtCostsConfig::test();
        eprintln!("Start SWEAT test");
        flush_disk_cache();
        let keys = generate_sweat_request_keys(&rocksdb);
        let nodes_before = trie.get_trie_nodes_count();
        warm_up_trie(trie);
        let start = Instant::now();
        let mut gas: Gas = 0;
        for key in keys.iter() {
            let contract_key =
                parse_data_key_from_contract_data_key(&key, &sweat_account_id()).unwrap();
            let value_ref = trie.get(key).unwrap();
            assert!(value_ref.is_some());
            gas += costs_config.gas_cost(ExtCosts::storage_write_base)
                + costs_config.gas_cost(ExtCosts::storage_write_key_byte)
                    * (contract_key.len() as u64);
        }
        eprintln!("Read {} keys in {:?}", keys.len(), start.elapsed());
        let nodes = trie.get_trie_nodes_count().checked_sub(&nodes_before).unwrap();
        gas += costs_config.gas_cost(ExtCosts::touching_trie_node) * nodes.db_reads
            + costs_config.gas_cost(ExtCosts::read_cached_trie_node) * nodes.mem_reads;
        eprintln!("Node reads: {nodes:?}, total gas: {}TGas", gas / 10e12 as u64);
        eprintln!("Finished SWEAT test");
        Ok(())
    }
}

fn warm_up_trie(trie: &Trie) {
    eprintln!("Start trie warmup");
    let start = Instant::now();
    trie.get_ref(&sweat_contract_data(vec![]), near_store::KeyLookupMode::Trie).unwrap();
    eprintln!("Finished trie warmup in {:?}", start.elapsed());
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
            if req_keys.keys.len() == REQUEST_COUNT {
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
    keys.shuffle(&mut StdRng::seed_from_u64(42));
    let req_keys = RequestKeys {
        keys: keys
            .iter()
            .take(REQUEST_COUNT)
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

fn sweat_shard() -> ShardUId {
    ShardLayout::get_simple_nightshade_layout().get_shard_uids()[3]
}

fn state_root(store: &Store) -> CryptoHash {
    let chunk: near_primitives::types::chunk_extra::ChunkExtra = store
        .get_ser(DBCol::ChunkExtra, &get_block_shard_uid(&flat_head(store), &sweat_shard()))
        .unwrap()
        .unwrap();
    chunk.state_root().clone()
}

fn flat_head(store: &Store) -> CryptoHash {
    match store_helper::get_flat_storage_status(store, sweat_shard()).unwrap() {
        near_store::flat::FlatStorageStatus::Ready(status) => status.flat_head.hash,
        other => panic!("invalid flat storage status {other:?}"),
    }
}

fn flush_disk_cache() {
    eprintln!("Flush disk page cache");
    let mut cmd = std::process::Command::new("sudo")
        .arg("tee")
        .arg("/proc/sys/vm/drop_caches")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    use std::io::Write;
    cmd.stdin.as_mut().unwrap().write_all("3".as_bytes()).unwrap();
    cmd.wait().unwrap();
}
