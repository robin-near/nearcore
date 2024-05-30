use crate::utils::read_all_state_needed_to_load_memtrie;
use borsh::BorshDeserialize;
use clap::Parser;
use indicatif::HumanBytes;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::{GenesisConfig, GenesisValidationMode};
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::state::FlatStateValue;
use near_store::flat::delta::KeyForFlatStateDelta;
use near_store::flat::store_helper::{decode_flat_state_db_key, encode_flat_state_db_key};
use near_store::flat::FlatStateChanges;
use near_store::trie::mem::loading::get_state_root;
use near_store::trie::mem::parallel_loader::make_memtrie_parallel_loading_plan;
use near_store::{DBCol, Store};
use nearcore::{load_config, open_storage};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// For developers only. Aggressively trims the database for testing purposes.
#[derive(Parser)]
pub(crate) struct AggressiveTrimmingCommand {
    #[clap(long)]
    obliterate_disk_trie: bool,
}

impl AggressiveTrimmingCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let mut near_config = load_config(home, GenesisValidationMode::UnsafeFast).unwrap();
        let node_storage = open_storage(&home, &mut near_config).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        if self.obliterate_disk_trie {
            Self::obliterate_disk_trie(store, &near_config.genesis.config)?;
        }
        Ok(())
    }

    /// Delete the entire State column except those that are not inlined by flat storage.
    /// This is used to TEST that we are able to rely on memtries only to run a node.
    /// It is NOT safe for production. Do not trim your nodes like this. It will break your node.
    fn obliterate_disk_trie(store: Store, genesis_config: &GenesisConfig) -> anyhow::Result<()> {
        let state_needed =
            read_all_state_needed_to_load_memtrie(store.clone(), genesis_config, true)?
                .state_entries;

        // Now that we've read all the non-inlined keys into memory, delete the State column, and
        // write back these values.
        let mut update = store.store_update();
        update.delete_all(DBCol::State);
        update.commit()?;

        let mut update = store.store_update();
        let mut total_bytes_written = 0;
        let mut total_bytes_written_this_batch = 0;
        let mut total_keys_written = 0;
        let total_bytes_to_write =
            state_needed.iter().map(|(key, value)| key.len() + value.len()).sum::<usize>();
        let total_keys_to_write = state_needed.len();
        for (key, value) in state_needed.iter() {
            update.set_raw_bytes(DBCol::State, key, value);
            total_bytes_written += key.len() + value.len();
            total_bytes_written_this_batch += key.len() + value.len();
            total_keys_written += 1;
            if total_bytes_written_this_batch > 100_000_000 {
                update.commit()?;
                update = store.store_update();
                total_bytes_written_this_batch = 0;
                println!(
                    "Written {} / {} keys ({} / {})",
                    total_keys_written,
                    total_keys_to_write,
                    HumanBytes(total_bytes_written as u64),
                    HumanBytes(total_bytes_to_write as u64)
                );
            }
        }
        update.commit()?;
        println!(
            "Done. Written {} / {} keys ({} / {})",
            total_keys_written,
            total_keys_to_write,
            HumanBytes(total_bytes_written as u64),
            HumanBytes(total_bytes_to_write as u64)
        );

        Ok(())
    }
}
