use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives::epoch_manager::epoch_info::{EpochInfo, EpochInfoV1};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, ExecutionOutcomeWithProof};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::AccountId;
use near_primitives::utils::get_outcome_id_block_hash;

use crate::{DBCol, Store, StoreUpdate};

pub struct BatchedStoreUpdate<'a> {
    batch_size_limit: usize,
    batch_size: usize,
    store: &'a Store,
    store_update: Option<StoreUpdate>,
}

impl<'a> BatchedStoreUpdate<'a> {
    pub fn new(store: &'a Store, batch_size_limit: usize) -> Self {
        Self { batch_size_limit, batch_size: 0, store, store_update: Some(store.store_update()) }
    }

    fn commit(&mut self) -> std::io::Result<()> {
        let store_update = self.store_update.take().unwrap();
        store_update.commit()?;
        self.store_update = Some(self.store.store_update());
        self.batch_size = 0;
        Ok(())
    }

    pub fn set_ser<T: BorshSerialize>(
        &mut self,
        col: DBCol,
        key: &[u8],
        value: &T,
    ) -> std::io::Result<()> {
        let value_bytes = value.try_to_vec()?;
        self.batch_size += key.as_ref().len() + value_bytes.len() + 8;
        self.store_update.as_mut().unwrap().set(col, key.as_ref(), &value_bytes);

        if self.batch_size > self.batch_size_limit {
            self.commit()?;
        }

        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<()> {
        if self.batch_size > 0 {
            self.commit()?;
        }

        Ok(())
    }
}

fn map_col<T, U, F>(store: &Store, col: DBCol, f: F) -> std::io::Result<()>
where
    T: BorshDeserialize,
    U: BorshSerialize,
    F: Fn(T) -> U,
{
    let mut store_update = BatchedStoreUpdate::new(store, 10_000_000);
    for pair in store.iter(col) {
        let (key, value) = pair?;
        let new_value = f(T::try_from_slice(&value).unwrap());
        store_update.set_ser(col, &key, &new_value)?;
    }
    store_update.finish()
}

/// Migrates database from version 28 to 29.
///
/// Deletes all data from _NextBlockWithNewChunk and _LastBlockWithNewChunk
/// columns.
pub fn migrate_28_to_29(storage: &crate::NodeStorage) -> anyhow::Result<()> {
    let mut update = storage.get_store(crate::Temperature::Hot).store_update();
    update.delete_all(DBCol::_NextBlockWithNewChunk);
    update.delete_all(DBCol::_LastBlockWithNewChunk);
    update.commit()?;
    Ok(())
}

/// Migrates database from version 29 to 30.
///
/// Migrates all structures that use ValidatorStake to versionized version.
pub fn migrate_29_to_30(storage: &crate::NodeStorage) -> anyhow::Result<()> {
    use near_primitives::epoch_manager::block_info::BlockInfo;
    use near_primitives::epoch_manager::epoch_info::EpochSummary;
    use near_primitives::epoch_manager::AGGREGATOR_KEY;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_primitives::types::validator_stake::ValidatorStakeV1;
    use near_primitives::types::{
        BlockChunkValidatorStats, EpochId, ProtocolVersion, ShardId, ValidatorId,
        ValidatorKickoutReason, ValidatorStats,
    };
    use std::collections::BTreeMap;

    let store = storage.get_store(crate::Temperature::Hot);

    #[derive(BorshDeserialize)]
    pub struct OldEpochSummary {
        pub prev_epoch_last_block_hash: CryptoHash,
        pub all_proposals: Vec<ValidatorStakeV1>,
        pub validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
        pub validator_block_chunk_stats: HashMap<AccountId, BlockChunkValidatorStats>,
        pub next_version: ProtocolVersion,
    }

    #[derive(BorshDeserialize)]
    pub struct OldEpochInfoAggregator {
        pub block_tracker: HashMap<ValidatorId, ValidatorStats>,
        pub shard_tracker: HashMap<ShardId, HashMap<ValidatorId, ValidatorStats>>,
        pub version_tracker: HashMap<ValidatorId, ProtocolVersion>,
        pub all_proposals: BTreeMap<AccountId, ValidatorStakeV1>,
        pub epoch_id: EpochId,
        pub last_block_hash: CryptoHash,
    }
    #[derive(BorshSerialize)]
    pub struct NewEpochInfoAggregator {
        pub block_tracker: HashMap<ValidatorId, ValidatorStats>,
        pub shard_tracker: HashMap<ShardId, HashMap<ValidatorId, ValidatorStats>>,
        pub version_tracker: HashMap<ValidatorId, ProtocolVersion>,
        pub all_proposals: BTreeMap<AccountId, ValidatorStake>,
        pub epoch_id: EpochId,
        pub last_block_hash: CryptoHash,
    }

    map_col(&store, DBCol::ChunkExtra, ChunkExtra::V1)?;

    map_col(&store, DBCol::BlockInfo, BlockInfo::V1)?;

    map_col(&store, DBCol::EpochValidatorInfo, |info: OldEpochSummary| EpochSummary {
        prev_epoch_last_block_hash: info.prev_epoch_last_block_hash,
        all_proposals: info.all_proposals.into_iter().map(ValidatorStake::V1).collect(),
        validator_kickout: info.validator_kickout,
        validator_block_chunk_stats: info.validator_block_chunk_stats,
        next_version: info.next_version,
    })?;

    // DBCol::EpochInfo has a special key which contains a different type than all other
    // values (EpochInfoAggregator), so we cannot use `map_col` on it. We need to handle
    // the AGGREGATOR_KEY differently from all others.
    let col = DBCol::EpochInfo;
    let keys = store
        .iter(col)
        .map(|item| item.map(|(key, _)| key))
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut store_update = BatchedStoreUpdate::new(&store, 10_000_000);
    for key in keys {
        if key.as_ref() == AGGREGATOR_KEY {
            let value: OldEpochInfoAggregator = store.get_ser(col, key.as_ref()).unwrap().unwrap();
            let new_value = NewEpochInfoAggregator {
                block_tracker: value.block_tracker,
                shard_tracker: value.shard_tracker,
                version_tracker: value.version_tracker,
                epoch_id: value.epoch_id,
                last_block_hash: value.last_block_hash,
                all_proposals: value
                    .all_proposals
                    .into_iter()
                    .map(|(account, stake)| (account, ValidatorStake::V1(stake)))
                    .collect(),
            };
            store_update.set_ser(col, key.as_ref(), &new_value)?;
        } else {
            let value: EpochInfoV1 = store.get_ser(col, key.as_ref()).unwrap().unwrap();
            let new_value = EpochInfo::V1(value);
            store_update.set_ser(col, key.as_ref(), &new_value)?;
        }
    }

    store_update.finish()?;
    Ok(())
}

/// Migrates database from version 31 to 32.
///
/// This involves deleting contents of ChunkPerHeightShard and GCCount columns
/// which are now deprecated and no longer used.
pub fn migrate_31_to_32(storage: &crate::NodeStorage) -> anyhow::Result<()> {
    let mut update = storage.get_store(crate::Temperature::Hot).store_update();
    update.delete_all(DBCol::_ChunkPerHeightShard);
    update.delete_all(DBCol::_GCCount);
    update.commit()?;
    Ok(())
}

fn get_row_count(store: &Store, col_name: &str) -> Option<i64> {
    let statistics = store.get_store_statistics()?;
    let col_stats = statistics.data.into_iter().find(|(col, _)| col == col_name)?.1;
    for stat in col_stats {
        match stat {
            crate::db::StatsValue::Count(count) => return Some(count),
            _ => {}
        }
    }
    None
}

/// Migrates database from version 32 to 33.
///
/// This removes the TransactionResult column and moves it to TransactionResultForBlock.
/// The new column removes the need for high-latency read-modify-write operations when committing
/// new blocks.
pub fn migrate_32_to_33(storage: &crate::NodeStorage) -> anyhow::Result<()> {
    const BATCH_SIZE: usize = 100000;
    let store = storage.get_store(crate::Temperature::Hot);
    let mut migrated_overall = 0;
    let num_rows = get_row_count(&store, "col7");
    loop {
        let mut update = store.store_update();
        let mut rows_migrated = 0;
        for row in store
            .iter_prefix_ser::<Vec<ExecutionOutcomeWithIdAndProof>>(DBCol::_TransactionResult, &[])
        {
            let (key, outcomes) = row?;
            update.delete(DBCol::_TransactionResult, &key);
            for outcome in outcomes {
                update.set_ser(
                    DBCol::TransactionResultForBlock,
                    &get_outcome_id_block_hash(outcome.id(), &outcome.block_hash),
                    &ExecutionOutcomeWithProof {
                        proof: outcome.proof,
                        outcome: outcome.outcome_with_id.outcome,
                    },
                )?;
            }

            rows_migrated += 1;
            if rows_migrated == BATCH_SIZE {
                break;
            }
        }
        if rows_migrated == 0 {
            break;
        } else {
            migrated_overall += rows_migrated;
            update.commit()?;
        }
        println!(
            "Migrated {}/{} TransactionResult rows",
            migrated_overall,
            num_rows.map(|rows| format!("{}", rows)).unwrap_or("unknown".to_string())
        );
    }
    Ok(())
}
