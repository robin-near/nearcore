use std::fs;
use std::path::Path;

use anyhow::anyhow;
use borsh::BorshDeserialize;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisConfig;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::hash::CryptoHash;
use near_primitives::state::FlatStateValue;
use near_store::flat::delta::KeyForFlatStateDelta;
use near_store::flat::store_helper::{
    decode_flat_state_db_key, encode_flat_state_db_key, get_flat_storage_status,
};
use near_store::flat::FlatStateChanges;
use near_store::trie::mem::loading::get_state_root;
use near_store::trie::mem::parallel_loader::{
    calculate_end_key, make_memtrie_parallel_loading_plan, MemtrieParallelLoadingPlan,
    TrieLoadingPlanNode, TrieLoadingPlanNodeKind,
};
use near_store::{DBCol, NodeStorage, Store};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::HashSet;
use strum::IntoEnumIterator;

pub(crate) fn open_rocksdb(
    home: &Path,
    mode: near_store::Mode,
) -> anyhow::Result<near_store::db::RocksDB> {
    let config = nearcore::config::Config::from_file_skip_validation(
        &home.join(nearcore::config::CONFIG_FILENAME),
    )?;
    let store_config = &config.store;
    let db_path = store_config.path.as_ref().cloned().unwrap_or_else(|| home.join("data"));
    let rocksdb =
        near_store::db::RocksDB::open(&db_path, store_config, mode, near_store::Temperature::Hot)?;
    Ok(rocksdb)
}

pub(crate) fn open_state_snapshot(home: &Path, mode: near_store::Mode) -> anyhow::Result<Store> {
    let config = nearcore::config::Config::from_file_skip_validation(
        &home.join(nearcore::config::CONFIG_FILENAME),
    )?;
    let store_config = &config.store;
    let db_path = store_config.path.as_ref().cloned().unwrap_or_else(|| home.join("data"));

    let state_snapshot_dir = db_path.join("state_snapshot");
    let snapshots: Result<Vec<_>, _> = fs::read_dir(state_snapshot_dir)?.into_iter().collect();
    let snapshots = snapshots?;
    let &[snapshot_dir] = &snapshots.as_slice() else {
        return Err(anyhow!("found more than one snapshot"));
    };

    let path = snapshot_dir.path();
    println!("state snapshot path {path:?}");

    let opener = NodeStorage::opener(&path, false, &store_config, None);
    let storage = opener.open_in_mode(mode)?;
    let store = storage.get_hot_store();

    Ok(store)
}

pub(crate) fn resolve_column(col_name: &str) -> anyhow::Result<DBCol> {
    DBCol::iter()
        .filter(|db_col| <&str>::from(db_col) == col_name)
        .next()
        .ok_or_else(|| anyhow!("column {col_name} does not exist"))
}

pub struct StateNeededToLoadMemTrie {
    pub state_entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub flat_state_ranges: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Reads all data from the State column that is necessary to load memtrie based on flat storage,
/// for all shards of the current sharding version. These include the values that are not inlined,
/// as well as some top trie nodes that are used during parallel memtrie loading.
pub fn read_all_state_needed_to_load_memtrie(
    store: Store,
    genesis_config: &GenesisConfig,
    include_flat_delta: bool,
) -> anyhow::Result<StateNeededToLoadMemTrie> {
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), genesis_config);
    let chain = ChainStore::new(store.clone(), genesis_config.genesis_height, false);
    let final_head = chain.final_head()?;
    let epoch_id = final_head.epoch_id;
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
    let shard_uids = shard_layout.shard_uids().collect::<Vec<_>>();

    let mut flat_state_ranges = Vec::new();

    let (non_inlined_keys_tx, non_inlined_keys_rx) =
        std::sync::mpsc::sync_channel::<Vec<u8>>(10000);
    let non_inlined_keys_collect_thread = std::thread::spawn(move || {
        let mut non_inlined_keys = Vec::new();
        while let Ok(key) = non_inlined_keys_rx.recv() {
            non_inlined_keys.push(key);
        }
        non_inlined_keys
    });
    for shard_uid in shard_uids {
        let flat_status = get_flat_storage_status(&store, shard_uid)?;
        let flat_head = match flat_status {
            near_store::flat::FlatStorageStatus::Ready(ready) => ready.flat_head.hash,
            _ => panic!("Flat storage is not ready for shard {shard_uid}"),
        };
        let root = get_state_root(&store, flat_head, shard_uid)?;
        let plan = make_memtrie_parallel_loading_plan(store.clone(), shard_uid, root)?;

        for hash in extract_hashes_needed_for_memtrie_loading_plan(&plan) {
            let key = encode_flat_state_db_key(shard_uid, &hash.0);
            non_inlined_keys_tx.send(key).unwrap();
        }

        // We'll tweak the loading ranges a bit, by making sure there are no gaps. This is because the
        // plan itself may be encoding some values, and we don't want to miss those when iterating flat
        // storage.
        let mut start_points = plan
            .subtrees_to_load
            .into_iter()
            .map(|subtree| {
                let (start, _) = subtree.to_iter_range(shard_uid);
                start
            })
            .collect::<Vec<_>>();

        let shard_uid_end_key = calculate_end_key(&shard_uid.to_bytes().to_vec(), 1).unwrap();
        let mut ranges = start_points
            .clone()
            .into_iter()
            .zip(start_points.into_iter().skip(1).chain(std::iter::once(shard_uid_end_key)))
            .collect::<Vec<_>>();
        flat_state_ranges.extend(ranges.clone());
        ranges.into_par_iter().for_each(|(start, end)| {
            for item in store.iter_range(DBCol::FlatState, Some(&start), Some(&end)) {
                let (key, value) = item.unwrap();
                let value = FlatStateValue::try_from_slice(&value).unwrap();
                match value {
                    FlatStateValue::Ref(r) => {
                        let (shard_uid, _) = decode_flat_state_db_key(&key).unwrap();
                        non_inlined_keys_tx.send(encode_flat_state_db_key(shard_uid, &r.hash.0));
                    }
                    FlatStateValue::Inlined(_) => {}
                }
            }
        });
    }

    if include_flat_delta {
        let shard_version_start_key = shard_layout.version().to_le_bytes().to_vec();
        let shard_version_end_key = calculate_end_key(&shard_version_start_key, 1).unwrap();
        for item in store.iter_range(
            DBCol::FlatStateChanges,
            Some(&shard_version_start_key),
            Some(&shard_version_end_key),
        ) {
            let (key, value) = item.unwrap();
            let changes = FlatStateChanges::try_from_slice(&value).unwrap();
            let key = KeyForFlatStateDelta::try_from_slice(&key).unwrap();
            for (_, value) in changes.0 {
                if let Some(FlatStateValue::Ref(r)) = value {
                    non_inlined_keys_tx.send(encode_flat_state_db_key(key.shard_uid, &r.hash.0));
                }
            }
        }
    }

    drop(non_inlined_keys_tx);
    let non_inlined_keys = non_inlined_keys_collect_thread.join().unwrap();
    println!(
        "Found {} non-inlined keys from FlatState and FlatStateChanges to read from State column",
        non_inlined_keys.len(),
    );

    let non_inlined_entries = non_inlined_keys
        .into_par_iter()
        .map(|key| {
            let value = store
                .get_raw_bytes(DBCol::State, &key)
                .unwrap_or_else(|e| panic!("Error reading key {key:?} from State column: {e}"))
                .unwrap_or_else(|| panic!("Key {key:?} not found in State column"));
            (key, value.to_vec())
        })
        .collect::<Vec<_>>();

    Ok(StateNeededToLoadMemTrie { state_entries: non_inlined_entries, flat_state_ranges })
}

fn extract_hashes_needed_for_memtrie_loading_plan_node(
    plan: &TrieLoadingPlanNode,
    hashes: &mut HashSet<CryptoHash>,
) {
    hashes.insert(plan.hash);
    match &plan.kind {
        TrieLoadingPlanNodeKind::Branch { children, .. } => {
            for (_, child) in children {
                extract_hashes_needed_for_memtrie_loading_plan_node(&child, hashes);
            }
        }
        TrieLoadingPlanNodeKind::Extension { child, .. } => {
            extract_hashes_needed_for_memtrie_loading_plan_node(&child, hashes);
        }
        _ => {}
    }
}

fn extract_hashes_needed_for_memtrie_loading_plan(
    plan: &MemtrieParallelLoadingPlan,
) -> Vec<CryptoHash> {
    let mut hashes = HashSet::new();
    extract_hashes_needed_for_memtrie_loading_plan_node(&plan.root, &mut hashes);
    let mut hashes = hashes.into_iter().collect::<Vec<_>>();
    hashes.sort();
    hashes
}
