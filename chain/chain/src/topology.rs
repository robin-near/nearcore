use std::sync::Arc;

use near_chain_primitives::Error;
use near_primitives::{
    block_header::BlockHeader,
    errors::InvalidTxError,
    hash::CryptoHash,
    merkle::PartialMerkleTree,
    receipt::Receipt,
    shard_layout::account_id_to_shard_id,
    sharding::{ShardChunk, ShardChunkHeader},
    syncing::ReceiptProofResponse,
    types::{BlockHeight, EpochId, NumBlocks, ShardId},
};

use crate::{byzantine_assert, ChainStore, ChainStoreAccess, RuntimeAdapter};

pub trait Topology {
    fn prev_block_is_caught_up(
        &self,
        prev_prev_hash: &CryptoHash,
        prev_hash: &CryptoHash,
    ) -> Result<bool, Error>;

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&self) -> Result<BlockHeader, Error>;

    /// Get previous header.
    fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error>;

    /// Get full chunk from header, with possible error that contains the header for further retrieval.
    fn get_chunk_clone_from_header(&self, header: &ShardChunkHeader) -> Result<ShardChunk, Error>;

    /// Returns hash of the first available block after genesis.
    fn get_earliest_block_hash(&self) -> Result<Option<CryptoHash>, Error>;

    /// Returns block header from the current chain for given height if present.
    fn get_block_header_by_height(&self, height: BlockHeight) -> Result<BlockHeader, Error>;

    /// Returns block header from the current chain defined by `sync_hash` for given height if present.
    fn get_block_header_on_chain_by_height(
        &self,
        sync_hash: &CryptoHash,
        height: BlockHeight,
    ) -> Result<BlockHeader, Error>;

    /// Collect incoming receipts for shard `shard_id` from
    /// the block at height `last_chunk_height_included` (non-inclusive) to the block `block_hash` (inclusive)
    /// This is because the chunks for the shard are empty for the blocks in between,
    /// so the receipts from these blocks are propagated
    fn get_incoming_receipts_for_shard(
        &self,
        shard_id: ShardId,
        block_hash: CryptoHash,
        last_chunk_height_included: BlockHeight,
    ) -> Result<Vec<ReceiptProofResponse>, Error>;

    fn get_block_merkle_tree_from_ordinal(
        &self,
        block_ordinal: NumBlocks,
    ) -> Result<Arc<PartialMerkleTree>, Error>;

    fn get_block_height(&self, hash: &CryptoHash) -> Result<BlockHeight, Error>;

    /// Get epoch id of the last block with existing chunk for the given shard id.
    fn get_epoch_id_of_last_block_with_chunk(
        &self,
        runtime_adapter: &dyn RuntimeAdapter,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<EpochId, Error>;
}

impl<T: ChainStoreAccess + ?Sized> Topology for T {
    fn prev_block_is_caught_up(
        &self,
        prev_prev_hash: &CryptoHash,
        prev_hash: &CryptoHash,
    ) -> Result<bool, Error> {
        // Needs to be used with care: for the first block of each epoch the semantic is slightly
        // different, since the prev_block is in a different epoch. So for all the blocks but the
        // first one in each epoch this method returns true if the block is ready to have state
        // applied for the next epoch, while for the first block in a particular epoch this method
        // returns true if the block is ready to have state applied for the current epoch (and
        // otherwise should be orphaned)
        Ok(!self.get_blocks_to_catchup(prev_prev_hash)?.contains(prev_hash))
    }

    fn head_header(&self) -> Result<BlockHeader, Error> {
        self.get_block_header(&(self.head()?.last_block_hash))
    }

    fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error> {
        self.get_block_header(header.prev_hash())
    }

    fn get_chunk_clone_from_header(&self, header: &ShardChunkHeader) -> Result<ShardChunk, Error> {
        let shard_chunk_result = self.get_chunk(&header.chunk_hash());
        match shard_chunk_result {
            Err(_) => {
                return Err(Error::ChunksMissing(vec![header.clone()]));
            }
            Ok(shard_chunk) => {
                byzantine_assert!(header.height_included() > 0 || header.height_created() == 0);
                if header.height_included() == 0 && header.height_created() > 0 {
                    return Err(Error::Other(format!(
                        "Invalid header: {:?} for chunk {:?}",
                        header, shard_chunk
                    )));
                }
                let mut shard_chunk_clone = ShardChunk::clone(&shard_chunk);
                shard_chunk_clone.set_height_included(header.height_included());
                Ok(shard_chunk_clone)
            }
        }
    }

    fn get_earliest_block_hash(&self) -> Result<Option<CryptoHash>, Error> {
        // To find the earliest available block we use the `tail` marker primarily
        // used by garbage collection system.
        // NOTE: `tail` is the block height at which we can say that there is
        // at most 1 block available in the range from the genesis height to
        // the tail. Thus, the strategy is to find the first block AFTER the tail
        // height, and use the `prev_hash` to get the reference to the earliest
        // block.
        // The earliest block can be the genesis block.
        let head_header_height = self.head_header()?.height();
        let tail = self.tail()?;

        // There is a corner case when there are no blocks after the tail, and
        // the tail is in fact the earliest block available on the chain.
        if let Ok(block_hash) = self.get_block_hash_by_height(tail) {
            return Ok(Some(block_hash));
        }
        for height in tail + 1..=head_header_height {
            if let Ok(block_hash) = self.get_block_hash_by_height(height) {
                let earliest_block_hash = *self.get_block_header(&block_hash)?.prev_hash();
                debug_assert!(matches!(self.block_exists(&earliest_block_hash), Ok(true)));
                return Ok(Some(earliest_block_hash));
            }
        }
        Ok(None)
    }

    fn get_block_header_by_height(&self, height: BlockHeight) -> Result<BlockHeader, Error> {
        let hash = self.get_block_hash_by_height(height)?;
        self.get_block_header(&hash)
    }

    fn get_block_header_on_chain_by_height(
        &self,
        sync_hash: &CryptoHash,
        height: BlockHeight,
    ) -> Result<BlockHeader, Error> {
        let mut header = self.get_block_header(sync_hash)?;
        let mut hash = *sync_hash;
        while header.height() > height {
            hash = *header.prev_hash();
            header = self.get_block_header(&hash)?;
        }
        let header_height = header.height();
        if header_height < height {
            return Err(Error::InvalidBlockHeight(header_height));
        }
        self.get_block_header(&hash)
    }

    fn get_incoming_receipts_for_shard(
        &self,
        shard_id: ShardId,
        mut block_hash: CryptoHash,
        last_chunk_height_included: BlockHeight,
    ) -> Result<Vec<ReceiptProofResponse>, Error> {
        let mut ret = vec![];

        loop {
            let header = self.get_block_header(&block_hash)?;

            if header.height() < last_chunk_height_included {
                panic!("get_incoming_receipts_for_shard failed");
            }

            if header.height() == last_chunk_height_included {
                break;
            }

            let prev_hash = *header.prev_hash();

            if let Ok(receipt_proofs) = self.get_incoming_receipts(&block_hash, shard_id) {
                ret.push(ReceiptProofResponse(block_hash, receipt_proofs));
            } else {
                ret.push(ReceiptProofResponse(block_hash, Arc::new(vec![])));
            }

            block_hash = prev_hash;
        }

        Ok(ret)
    }

    fn get_block_merkle_tree_from_ordinal(
        &self,
        block_ordinal: NumBlocks,
    ) -> Result<Arc<PartialMerkleTree>, Error> {
        let block_hash = self.get_block_hash_from_ordinal(block_ordinal)?;
        self.get_block_merkle_tree(&block_hash)
    }

    fn get_block_height(&self, hash: &CryptoHash) -> Result<BlockHeight, Error> {
        if hash == &CryptoHash::default() {
            Ok(self.get_genesis_height())
        } else {
            Ok(self.get_block_header(hash)?.height())
        }
    }

    fn get_epoch_id_of_last_block_with_chunk(
        &self,
        runtime_adapter: &dyn RuntimeAdapter,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<EpochId, Error> {
        let mut candidate_hash = *hash;
        let mut shard_id = shard_id;
        loop {
            let block_header = self.get_block_header(&candidate_hash)?;
            if block_header.chunk_mask()[shard_id as usize] {
                break Ok(block_header.epoch_id().clone());
            }
            candidate_hash = *block_header.prev_hash();
            shard_id = runtime_adapter.get_prev_shard_ids(&candidate_hash, vec![shard_id])?[0];
        }
    }
}

impl ChainStore {
    /// Get outgoing receipts that will be *sent* from shard `shard_id` from block whose prev block
    /// is `prev_block_hash`
    /// Note that the meaning of outgoing receipts here are slightly different from
    /// `save_outgoing_receipts` or `get_outgoing_receipts`.
    /// There, outgoing receipts for a shard refers to receipts that are generated
    /// from the shard from block `prev_block_hash`.
    /// Here, outgoing receipts for a shard refers to receipts that will be sent from this shard
    /// to other shards in the block after `prev_block_hash`
    /// The difference of one block is important because shard layout may change between the previous
    /// block and the current block and the meaning of `shard_id` will change.
    ///
    /// Note, the current way of implementation assumes that at least one chunk is generated before
    /// shard layout are changed twice. This is not a problem right now because we are changing shard
    /// layout for the first time for simple nightshade and generally not a problem if shard layout
    /// changes very rarely.
    /// But we need to implement a more theoretically correct algorithm if shard layouts will change
    /// more often in the future
    /// <https://github.com/near/nearcore/issues/4877>
    pub fn get_outgoing_receipts_for_shard(
        &self,
        runtime_adapter: &dyn RuntimeAdapter,
        prev_block_hash: CryptoHash,
        shard_id: ShardId,
        last_included_height: BlockHeight,
    ) -> Result<Vec<Receipt>, Error> {
        let shard_layout = runtime_adapter.get_shard_layout_from_prev_block(&prev_block_hash)?;
        let mut receipts_block_hash = prev_block_hash;
        loop {
            let block_header = self.get_block_header(&receipts_block_hash)?;

            if block_header.height() == last_included_height {
                let receipts_shard_layout =
                    runtime_adapter.get_shard_layout(block_header.epoch_id())?;

                // get the shard from which the outgoing receipt were generated
                let receipts_shard_id = if shard_layout != receipts_shard_layout {
                    shard_layout.get_parent_shard_id(shard_id)?
                } else {
                    shard_id
                };
                let mut receipts = self
                    .get_outgoing_receipts(&receipts_block_hash, receipts_shard_id)
                    .map(|v| v.to_vec())
                    .unwrap_or_default();

                // filter to receipts that belong to `shard_id` in the current shard layout
                if shard_layout != receipts_shard_layout {
                    receipts.retain(|receipt| {
                        account_id_to_shard_id(&receipt.receiver_id, &shard_layout) == shard_id
                    });
                }

                return Ok(receipts);
            } else {
                receipts_block_hash = *block_header.prev_hash();
            }
        }
    }

    /// For a given transaction, it expires if the block that the chunk points to is more than `validity_period`
    /// ahead of the block that has `base_block_hash`.
    pub fn check_transaction_validity_period(
        &self,
        prev_block_header: &BlockHeader,
        base_block_hash: &CryptoHash,
        validity_period: BlockHeight,
    ) -> Result<(), InvalidTxError> {
        // if both are on the canonical chain, comparing height is sufficient
        // we special case this because it is expected that this scenario will happen in most cases.
        let base_height =
            self.get_block_header(base_block_hash).map_err(|_| InvalidTxError::Expired)?.height();
        let prev_height = prev_block_header.height();
        if let Ok(base_block_hash_by_height) = self.get_block_hash_by_height(base_height) {
            if &base_block_hash_by_height == base_block_hash {
                if let Ok(prev_hash) = self.get_block_hash_by_height(prev_height) {
                    if &prev_hash == prev_block_header.hash() {
                        if prev_height <= base_height + validity_period {
                            return Ok(());
                        } else {
                            return Err(InvalidTxError::Expired);
                        }
                    }
                }
            }
        }

        // if the base block height is smaller than `last_final_height` we only need to check
        // whether the base block is the same as the one with that height on the canonical fork.
        // Otherwise we walk back the chain to check whether base block is on the same chain.
        let last_final_height = self
            .get_block_height(prev_block_header.last_final_block())
            .map_err(|_| InvalidTxError::InvalidChain)?;

        if prev_height > base_height + validity_period {
            Err(InvalidTxError::Expired)
        } else if last_final_height >= base_height {
            let base_block_hash_by_height = self
                .get_block_hash_by_height(base_height)
                .map_err(|_| InvalidTxError::InvalidChain)?;
            if &base_block_hash_by_height == base_block_hash {
                if prev_height <= base_height + validity_period {
                    Ok(())
                } else {
                    Err(InvalidTxError::Expired)
                }
            } else {
                Err(InvalidTxError::InvalidChain)
            }
        } else {
            let header = self
                .get_block_header_on_chain_by_height(prev_block_header.hash(), base_height)
                .map_err(|_| InvalidTxError::InvalidChain)?;
            if header.hash() == base_block_hash {
                Ok(())
            } else {
                Err(InvalidTxError::InvalidChain)
            }
        }
    }
}
