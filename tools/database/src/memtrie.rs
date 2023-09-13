use crate::utils::{flat_head_state_root, open_rocksdb};
use near_epoch_manager::EpochManager;
use near_primitives::block::Tip;
use near_primitives::block_header::BlockHeader;
use near_primitives::borsh::BorshSerialize;
use near_primitives::types::ShardId;
use near_store::trie::mem::loading::load_trie_from_flat_state;
use near_store::{DBCol, NibbleSlice, ShardUId, HEAD_KEY};
use nearcore::NearConfig;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[derive(clap::Parser)]
pub struct MemTrieCmd {
    #[clap(long)]
    shard_id: ShardId,
}

impl MemTrieCmd {
    pub fn run(&self, near_config: NearConfig, home: &Path) -> anyhow::Result<()> {
        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadOnly)?);
        let store = near_store::NodeStorage::new(rocksdb.clone()).get_hot_store();
        let genesis_config = &near_config.genesis.config;
        // Note: this is not necessarily correct; it's just an estimate of the shard layout.
        let head =
            store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY).unwrap().unwrap().last_block_hash;
        let block_header = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, &head.try_to_vec().unwrap())?
            .ok_or_else(|| anyhow::anyhow!("Block header not found"))?;
        let epoch_manager =
            EpochManager::new_from_genesis_config(store.clone(), &genesis_config).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(block_header.epoch_id()).unwrap();

        let shard_uid = ShardUId::from_shard_id_and_layout(self.shard_id, &shard_layout);
        let state_root = flat_head_state_root(&store, &shard_uid);

        let _trie = load_trie_from_flat_state(&store, shard_uid, state_root)?;
        for _ in 0..1000000 {
            std::thread::sleep(Duration::from_secs(100));
        }
        Ok(())
    }
}
