use indicatif::{HumanBytes, ParallelProgressIterator};
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisConfig;
use near_database_tool::utils::read_all_state_needed_to_load_memtrie;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::{borsh::BorshDeserialize, state::FlatStateValue};
use near_store::flat::store_helper::{decode_flat_state_db_key, encode_flat_state_db_key};
use near_store::{DBCol, Store};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

/// Trims the database for forknet by rebuilding the entire database with only the FlatState
/// and the small part of State that is needed for flat state, i.e. the non-inlined trie values.
/// Also copies over the basic DbVersion and Misc columns.
///
/// Does not mutate the old store, but populates the new store with the trimmed data.
///
/// Note that only the state in the FlatState is copied. All other state in FlatStateChanges is
/// ignored. For this reason, this is only suitable for forknet testing.
pub fn trim_database(old: Store, genesis_config: &GenesisConfig, new: Store) -> anyhow::Result<()> {
    let state_needed = read_all_state_needed_to_load_memtrie(old.clone(), genesis_config, false)?;

    tracing::info!("Copying FlatState to new database...");
    state_needed.flat_state_ranges.into_par_iter().progress().for_each(|(start, end)| {
        let mut update = new.store_update();
        for item in old.iter_range(DBCol::FlatState, Some(&start), Some(&end)) {
            let (key, value) = item.unwrap();
            update.set_raw_bytes(DBCol::FlatState, &key, &value);
        }
        update.commit().unwrap();
    });
    tracing::info!("Done copying FlatState to new database");

    let mut update = new.store_update();
    let mut keys_copied = 0;
    let mut bytes_copied = 0;
    let mut bytes_this_batch = 0;
    let mut last_print = std::time::Instant::now();
    for (key, value) in state_needed.state_entries {
        update.set_raw_bytes(DBCol::State, &key, &value);
        keys_copied += 1;
        bytes_copied += key.len() + value.len();
        bytes_this_batch += key.len() + value.len();
        if bytes_this_batch > 100 * 1024 * 1024 {
            update.commit()?;
            update = new.store_update();
            bytes_this_batch = 0;
        }
        if last_print.elapsed().as_secs() > 3 {
            println!(
                "State: Copied {} keys, {} bytes",
                keys_copied,
                HumanBytes(bytes_copied as u64)
            );
            last_print = std::time::Instant::now();
        }
    }
    update.commit()?;
    println!(
        "Done copying State: Copied {} keys, {} bytes",
        keys_copied,
        HumanBytes(bytes_copied as u64)
    );

    // Finally copy over some metadata columns
    let mut update = new.store_update();
    for item in old.iter_raw_bytes(DBCol::DbVersion) {
        let (key, value) = item?;
        update.set_raw_bytes(DBCol::DbVersion, &key, &value);
    }
    for item in old.iter_raw_bytes(DBCol::Misc) {
        let (key, value) = item?;
        update.set_raw_bytes(DBCol::Misc, &key, &value);
    }
    update.commit()?;
    println!("Done constructing trimmed database");

    Ok(())
}
