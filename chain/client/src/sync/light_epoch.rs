use crate::client_actor::ClientActorInner;
use borsh::BorshDeserialize;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{CanSend, Handler};
use near_async::time::Clock;
use near_chain::types::Tip;
use near_chain::{BlockHeader, Chain, ChainStoreAccess, Error};
use near_chain_configs::LightEpochSyncConfig;
use near_client_primitives::types::{LightEpochSyncStatus, SyncStatus};
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::{EpochSyncRequestMessage, EpochSyncResponseMessage};
use near_network::types::{
    HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_performance_metrics_macros::perf;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::hash::CryptoHash;
use near_primitives::light_epoch_sync::{
    EpochSyncProof, EpochSyncProofCurrentEpochData, EpochSyncProofLastEpochData,
    EpochSyncProofPastEpochData,
};
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::network::PeerId;
use near_primitives::types::{BlockHeight, EpochId};
use near_store::{DBCol, Store, FINAL_HEAD_KEY};
use rand::seq::SliceRandom;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;

pub struct LightEpochSync {
    clock: Clock,
    network_adapter: PeerManagerAdapter,
    genesis: BlockHeader,
    async_computation_spawner: Arc<dyn AsyncComputationSpawner>,
    config: LightEpochSyncConfig,
}

impl LightEpochSync {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        genesis: BlockHeader,
        async_computation_spawner: Arc<dyn AsyncComputationSpawner>,
        config: LightEpochSyncConfig,
    ) -> Self {
        Self { clock, network_adapter, genesis, async_computation_spawner, config }
    }

    fn derive_epoch_sync_proof(store: Store) -> Result<EpochSyncProof, Error> {
        let tip = store
            .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?
            .ok_or_else(|| Error::Other("Could not find tip".to_string()))?;
        let next_next_epoch_id = tip.next_epoch_id;
        let target_epoch_last_block_header = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, next_next_epoch_id.0.as_bytes())?
            .ok_or_else(|| Error::Other("Could not find last block of target epoch".to_string()))?;
        let target_epoch_second_last_block_header = store
            .get_ser::<BlockHeader>(
                DBCol::BlockHeader,
                target_epoch_last_block_header.prev_hash().as_bytes(),
            )?
            .ok_or_else(|| {
                Error::Other("Could not find second last block of target epoch".to_string())
            })?;

        Self::derive_epoch_sync_proof_from_final_block(store, target_epoch_second_last_block_header)
    }

    #[instrument(skip(store), level = "info")]
    fn derive_epoch_sync_proof_from_final_block(
        store: Store,
        next_block_header_after_final_block_in_current_epoch: BlockHeader,
    ) -> Result<EpochSyncProof, Error> {
        let final_block_header_in_current_epoch = store
            .get_ser::<BlockHeader>(
                DBCol::BlockHeader,
                next_block_header_after_final_block_in_current_epoch.prev_hash().as_bytes(),
            )?
            .ok_or_else(|| Error::Other("Could not find final block header".to_string()))?;

        let current_epoch = *final_block_header_in_current_epoch.epoch_id();
        let current_epoch_info = store
            .get_ser::<EpochInfo>(DBCol::EpochInfo, current_epoch.0.as_bytes())?
            .ok_or_else(|| Error::EpochOutOfBounds(current_epoch))?;
        let next_epoch = *final_block_header_in_current_epoch.next_epoch_id();
        let next_epoch_info = store
            .get_ser::<EpochInfo>(DBCol::EpochInfo, next_epoch.0.as_bytes())?
            .ok_or_else(|| Error::EpochOutOfBounds(next_epoch))?;

        // TODO: don't always generate from genesis
        let all_past_epochs = Self::get_all_past_epochs(
            &store,
            EpochId::default(),
            next_epoch,
            &final_block_header_in_current_epoch,
        )?;
        if all_past_epochs.is_empty() {
            return Err(Error::Other("Need at least three epochs to epoch sync".to_string()));
        }
        let prev_epoch = all_past_epochs.last().unwrap().epoch_id;
        let prev_epoch_info = store
            .get_ser::<EpochInfo>(DBCol::EpochInfo, prev_epoch.0.as_bytes())?
            .ok_or_else(|| Error::EpochOutOfBounds(prev_epoch))?;

        let last_block_of_prev_epoch = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, next_epoch.0.as_bytes())?
            .ok_or_else(|| Error::Other("Could not find last block of target epoch".to_string()))?;

        let last_block_info_of_prev_epoch = store
            .get_ser::<BlockInfo>(DBCol::BlockInfo, last_block_of_prev_epoch.hash().as_bytes())?
            .ok_or_else(|| Error::Other("Could not find last block info".to_string()))?;

        let second_last_block_of_prev_epoch = store
            .get_ser::<BlockHeader>(
                DBCol::BlockHeader,
                last_block_of_prev_epoch.prev_hash().as_bytes(),
            )?
            .ok_or_else(|| {
                Error::Other("Could not find second last block of target epoch".to_string())
            })?;

        let second_last_block_info_of_prev_epoch = store
            .get_ser::<BlockInfo>(
                DBCol::BlockInfo,
                last_block_of_prev_epoch.prev_hash().as_bytes(),
            )?
            .ok_or_else(|| Error::Other("Could not find second last block info".to_string()))?;

        let first_block_info_of_prev_epoch = store
            .get_ser::<BlockInfo>(
                DBCol::BlockInfo,
                last_block_info_of_prev_epoch.epoch_first_block().as_bytes(),
            )?
            .ok_or_else(|| Error::Other("Could not find first block info".to_string()))?;

        let block_info_for_final_block_of_current_epoch = store
            .get_ser::<BlockInfo>(
                DBCol::BlockInfo,
                final_block_header_in_current_epoch.hash().as_bytes(),
            )?
            .ok_or_else(|| {
                Error::Other("Could not find block info for latest final block".to_string())
            })?;

        let first_block_of_current_epoch = store
            .get_ser::<BlockHeader>(
                DBCol::BlockHeader,
                block_info_for_final_block_of_current_epoch.epoch_first_block().as_bytes(),
            )?
            .ok_or_else(|| Error::Other("Could not find first block of next epoch".to_string()))?;

        let first_block_info_of_current_epoch = store
            .get_ser::<BlockInfo>(
                DBCol::BlockInfo,
                block_info_for_final_block_of_current_epoch.epoch_first_block().as_bytes(),
            )?
            .ok_or_else(|| {
                Error::Other("Could not find first block info of next epoch".to_string())
            })?;

        let merkle_proof_for_first_block_of_current_epoch = store
            .get_ser::<PartialMerkleTree>(
                DBCol::BlockMerkleTree,
                first_block_of_current_epoch.hash().as_bytes(),
            )?
            .ok_or_else(|| {
                Error::Other("Could not find merkle proof for first block".to_string())
            })?;

        let proof = EpochSyncProof {
            epoch_id: prev_epoch,
            past_epochs: all_past_epochs,
            last_epoch: EpochSyncProofLastEpochData {
                epoch_info: prev_epoch_info,
                next_epoch_info: current_epoch_info,
                next_next_epoch_info: next_epoch_info,
                first_block_in_epoch: first_block_info_of_prev_epoch,
                last_block_in_epoch: last_block_info_of_prev_epoch,
                second_last_block_in_epoch: second_last_block_info_of_prev_epoch,
                final_block_header_in_next_epoch: final_block_header_in_current_epoch,
                approvals_for_final_block_in_next_epoch:
                    next_block_header_after_final_block_in_current_epoch.approvals().to_vec(),
            },
            current_epoch: EpochSyncProofCurrentEpochData {
                first_block_header_in_epoch: first_block_of_current_epoch,
                first_block_info_in_epoch: first_block_info_of_current_epoch,
                last_block_header_in_prev_epoch: last_block_of_prev_epoch,
                second_last_block_header_in_prev_epoch: second_last_block_of_prev_epoch,
                merkle_proof_for_first_block: merkle_proof_for_first_block_of_current_epoch,
            },
        };
        let serialized = borsh::to_vec(&proof).unwrap();
        tracing::info!("Size of epoch sync proof: {}", serialized.len());

        Ok(proof)
    }

    #[instrument(skip(store, last_epoch_any_header), level = "info")]
    fn get_all_past_epochs(
        store: &Store,
        after_epoch: EpochId,
        before_epoch: EpochId,
        last_epoch_any_header: &BlockHeader,
    ) -> Result<Vec<EpochSyncProofPastEpochData>, Error> {
        let mut all_epoch_infos = HashMap::new();
        for item in store.iter(DBCol::EpochInfo) {
            let (key, value) = item?;
            if key.as_ref() == AGGREGATOR_KEY {
                continue;
            }
            let epoch_id = EpochId::try_from_slice(key.as_ref())?;
            let epoch_info = EpochInfo::try_from_slice(value.as_ref())?;
            all_epoch_infos.insert(epoch_id, epoch_info);
        }

        let mut epoch_to_prev_epoch = HashMap::new();

        epoch_to_prev_epoch
            .insert(*last_epoch_any_header.next_epoch_id(), *last_epoch_any_header.epoch_id());
        for (epoch_id, _) in &all_epoch_infos {
            if let Ok(Some(block)) =
                store.get_ser::<BlockHeader>(DBCol::BlockHeader, epoch_id.0.as_bytes())
            {
                epoch_to_prev_epoch.insert(*block.next_epoch_id(), *block.epoch_id());
            }
        }

        let mut epoch_ids = vec![];

        let mut current_epoch = before_epoch;
        while current_epoch != after_epoch {
            let prev_epoch = epoch_to_prev_epoch.get(&current_epoch).ok_or_else(|| {
                Error::Other(format!("Could not find prev epoch for {:?}", current_epoch))
            })?;
            epoch_ids.push(current_epoch);
            current_epoch = *prev_epoch;
        }
        epoch_ids.reverse();

        tracing::info!("Fetching data for {} past epochs", epoch_ids.len() - 2);
        let epochs = (0..epoch_ids.len() - 2)
            .into_par_iter()
            .map(|index| -> Result<EpochSyncProofPastEpochData, Error> {
                let next_next_epoch_id = epoch_ids[index + 2];
                let epoch_id = epoch_ids[index];
                let last_block_header = store
                    .get_ser::<BlockHeader>(DBCol::BlockHeader, next_next_epoch_id.0.as_bytes())?
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "Could not find last block header for epoch {:?}",
                            epoch_id
                        ))
                    })?;
                let second_last_block_header = store
                    .get_ser::<BlockHeader>(
                        DBCol::BlockHeader,
                        last_block_header.prev_hash().as_bytes(),
                    )?
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "Could not find second last block header for epoch {:?}",
                            epoch_id
                        ))
                    })?;
                let third_last_block_header = store
                    .get_ser::<BlockHeader>(
                        DBCol::BlockHeader,
                        second_last_block_header.prev_hash().as_bytes(),
                    )?
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "Could not find third last block header for epoch {:?}",
                            epoch_id
                        ))
                    })?;
                let epoch_info = all_epoch_infos.get(&epoch_id).ok_or_else(|| {
                    Error::Other(format!("Could not find epoch info for epoch {:?}", epoch_id))
                })?;
                Ok(EpochSyncProofPastEpochData {
                    epoch_id,
                    block_producers: epoch_info
                        .block_producers_settlement()
                        .iter()
                        .map(|validator_id| epoch_info.get_validator(*validator_id))
                        .collect(),
                    last_final_block_header: third_last_block_header,
                    approvals_for_last_final_block: second_last_block_header.approvals().to_vec(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(epochs)
    }

    pub fn run(
        &self,
        status: &mut SyncStatus,
        chain: &Chain,
        highest_height: BlockHeight,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<(), Error> {
        let tip_height = chain.chain_store().header_head()?.height;
        if tip_height + self.config.epoch_sync_horizon >= highest_height {
            return Ok(());
        }
        match status {
            SyncStatus::AwaitingPeers | SyncStatus::StateSync(_) => {
                return Ok(());
            }
            SyncStatus::LightEpochSync(status) => {
                if status.attempt_time + self.config.timeout_for_epoch_sync < self.clock.now_utc() {
                    tracing::warn!("Epoch sync from {} timed out; retrying", status.source_peer_id);
                } else {
                    return Ok(());
                }
            }
            _ => {}
        }

        let peer = highest_height_peers
            .choose(&mut rand::thread_rng())
            .ok_or_else(|| Error::Other("No peers to request epoch sync from".to_string()))?;

        *status = SyncStatus::LightEpochSync(LightEpochSyncStatus {
            source_peer_id: peer.peer_info.id.clone(),
            source_peer_height: peer.highest_block_height,
            attempt_time: self.clock.now_utc(),
        });

        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::EpochSyncRequest { peer_id: peer.peer_info.id.clone() },
        ));

        Ok(())
    }

    pub fn apply_proof(
        &self,
        status: &mut SyncStatus,
        chain: &mut Chain,
        proof: EpochSyncProof,
        source_peer: PeerId,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Result<(), Error> {
        if let SyncStatus::LightEpochSync(status) = status {
            if status.source_peer_id != source_peer {
                tracing::warn!("Ignoring epoch sync proof from unexpected peer: {}", source_peer);
                return Ok(());
            }
            if proof.current_epoch.first_block_header_in_epoch.height()
                + self.config.epoch_sync_accept_proof_max_horizon
                < status.source_peer_height
            {
                tracing::error!(
                    "Ignoring epoch sync proof from peer {} with too high height",
                    source_peer
                );
                return Ok(());
            }
        } else {
            tracing::warn!("Ignoring unexpected epoch sync proof from peer: {}", source_peer);
            return Ok(());
        }

        let last_header = proof.current_epoch.first_block_header_in_epoch;
        let mut store_update = chain.chain_store.store().store_update();

        let mut update = chain.mut_chain_store().store_update();
        update.save_block_header_no_update_tree(last_header.clone())?;
        update.save_block_header_no_update_tree(
            proof.current_epoch.last_block_header_in_prev_epoch,
        )?;
        update.save_block_header_no_update_tree(
            proof.current_epoch.second_last_block_header_in_prev_epoch,
        )?;
        tracing::info!(
            "last final block of last past epoch: {:?}",
            proof.past_epochs.last().unwrap().last_final_block_header.hash()
        );
        update.save_block_header_no_update_tree(
            proof.past_epochs.last().unwrap().last_final_block_header.clone(),
        )?;
        update.force_save_header_head(&Tip::from_header(&last_header))?;
        update.save_final_head(&Tip::from_header(&self.genesis))?;

        store_update.set_ser(
            DBCol::EpochInfo,
            &borsh::to_vec(proof.last_epoch.last_block_in_epoch.epoch_id()).unwrap(),
            &proof.last_epoch.epoch_info,
        )?;

        store_update.set_ser(
            DBCol::EpochInfo,
            &borsh::to_vec(proof.last_epoch.final_block_header_in_next_epoch.epoch_id()).unwrap(),
            &proof.last_epoch.next_epoch_info,
        )?;

        store_update.set_ser(
            DBCol::EpochInfo,
            &borsh::to_vec(proof.last_epoch.final_block_header_in_next_epoch.next_epoch_id())
                .unwrap(),
            &proof.last_epoch.next_next_epoch_info,
        )?;

        store_update.insert_ser(
            DBCol::BlockInfo,
            &borsh::to_vec(proof.last_epoch.first_block_in_epoch.hash()).unwrap(),
            &proof.last_epoch.first_block_in_epoch,
        )?;

        store_update.insert_ser(
            DBCol::BlockInfo,
            &borsh::to_vec(proof.last_epoch.last_block_in_epoch.hash()).unwrap(),
            &proof.last_epoch.last_block_in_epoch,
        )?;

        store_update.insert_ser(
            DBCol::BlockInfo,
            &borsh::to_vec(proof.last_epoch.second_last_block_in_epoch.hash()).unwrap(),
            &proof.last_epoch.second_last_block_in_epoch,
        )?;

        store_update.insert_ser(
            DBCol::BlockInfo,
            &borsh::to_vec(proof.current_epoch.first_block_info_in_epoch.hash()).unwrap(),
            &proof.current_epoch.first_block_info_in_epoch,
        )?;

        store_update.set_ser(
            DBCol::EpochStart,
            last_header.epoch_id().0.as_bytes(),
            &last_header.height(),
        )?;

        store_update.set_ser(
            DBCol::BlockOrdinal,
            &borsh::to_vec(&proof.current_epoch.merkle_proof_for_first_block.size()).unwrap(),
            last_header.hash(),
        )?;

        store_update.set_ser(
            DBCol::BlockHeight,
            &borsh::to_vec(&last_header.height()).unwrap(),
            last_header.hash(),
        )?;

        store_update.set_ser(
            DBCol::BlockMerkleTree,
            last_header.hash().as_bytes(),
            &proof.current_epoch.merkle_proof_for_first_block,
        )?;

        update.merge(store_update);

        update.commit()?;

        epoch_manager.force_update_aggregator(
            proof.last_epoch.first_block_in_epoch.epoch_id(),
            proof.last_epoch.last_block_in_epoch.last_final_block_hash(),
        )?;

        *status = SyncStatus::NoSync;

        Ok(())
    }
}

impl Handler<EpochSyncRequestMessage> for ClientActorInner {
    #[perf]
    fn handle(&mut self, msg: EpochSyncRequestMessage) {
        let store = self.client.chain.chain_store.store().clone();
        let network_adapter = self.client.network_adapter.clone();
        let route_back = msg.route_back;
        self.client.light_epoch_sync.async_computation_spawner.spawn(
            "respond to epoch sync request",
            move || {
                let proof = match LightEpochSync::derive_epoch_sync_proof(store) {
                    Ok(epoch_sync_proof) => epoch_sync_proof,
                    Err(e) => {
                        tracing::error!("Failed to derive epoch sync proof: {:?}", e);
                        return;
                    }
                };
                network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::EpochSyncResponse { route_back, proof },
                ));
            },
        )
    }
}

impl Handler<EpochSyncResponseMessage> for ClientActorInner {
    #[perf]
    fn handle(&mut self, msg: EpochSyncResponseMessage) {
        if let Err(e) = self.client.light_epoch_sync.apply_proof(
            &mut self.client.sync_status,
            &mut self.client.chain,
            msg.proof,
            msg.from_peer,
            self.client.epoch_manager.as_ref(),
        ) {
            tracing::error!("Failed to apply epoch sync proof: {:?}", e);
        }
    }
}
