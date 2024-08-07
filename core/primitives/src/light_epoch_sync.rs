use crate::block_header::BlockHeader;
use crate::epoch_manager::block_info::BlockInfo;
use crate::epoch_manager::epoch_info::EpochInfo;
use crate::merkle::PartialMerkleTree;
use crate::types::validator_stake::ValidatorStake;
use crate::types::EpochId;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::Signature;

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSyncProof {
    pub epoch_id: EpochId,
    pub past_epochs: Vec<EpochSyncProofPastEpochData>,
    pub last_epoch: EpochSyncProofLastEpochData,
    pub current_epoch: EpochSyncProofCurrentEpochData,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSyncProofPastEpochData {
    pub epoch_id: EpochId,
    pub block_producers: Vec<ValidatorStake>,
    pub last_final_block_header: BlockHeader,
    pub approvals_for_last_final_block: Vec<Option<Box<Signature>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSyncProofLastEpochData {
    pub epoch_info: EpochInfo,
    pub next_epoch_info: EpochInfo,
    pub next_next_epoch_info: EpochInfo,
    pub first_block_in_epoch: BlockInfo,
    pub last_block_in_epoch: BlockInfo,
    pub second_last_block_in_epoch: BlockInfo,
    pub final_block_header_in_next_epoch: BlockHeader,
    pub approvals_for_final_block_in_next_epoch: Vec<Option<Box<Signature>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSyncProofCurrentEpochData {
    pub first_block_header_in_epoch: BlockHeader,
    pub first_block_info_in_epoch: BlockInfo,
    pub last_block_header_in_prev_epoch: BlockHeader,
    pub second_last_block_header_in_prev_epoch: BlockHeader,
    // TODO: I'm not sure if this can be used to prove the block against the merkle root
    // included in the final block in this next epoch (included in LastEpochData).
    pub merkle_proof_for_first_block: PartialMerkleTree,
}
