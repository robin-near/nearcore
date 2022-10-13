use std::collections::HashMap;

use actix::Message;
use near_chain::types::Tip;
use near_network::types::{
    MsgRecipient, PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg,
    PartialEncodedChunkResponseMsg,
};
use near_pool::{PoolIteratorWrapper, TransactionPool};
use near_primitives::{
    epoch_manager::RngSeed,
    hash::CryptoHash,
    merkle::MerklePath,
    receipt::Receipt,
    sharding::{EncodedShardChunk, PartialEncodedChunk, ShardChunk, ShardChunkHeader},
    transaction::SignedTransaction,
    types::{EpochId, ShardId},
};

pub trait ClientAdapterForShardsManager: Send + Sync + 'static {
    fn did_complete_chunk(
        &self,
        partial_chunk: PartialEncodedChunk,
        shard_chunk: Option<ShardChunk>,
    );
    fn saw_invalid_chunk(&self, chunk: EncodedShardChunk);
    fn chunk_header_ready_for_inclusion(&self, chunk_header: ShardChunkHeader);
}

#[derive(Message)]
#[rtype(result = "()")]
pub enum ShardsManagerResponse {
    ChunkCompleted { partial_chunk: PartialEncodedChunk, shard_chunk: Option<ShardChunk> },
    InvalidChunk(EncodedShardChunk),
    ChunkHeaderReadyForInclusion(ShardChunkHeader),
}

impl<A: MsgRecipient<ShardsManagerResponse>> ClientAdapterForShardsManager for A {
    fn did_complete_chunk(
        &self,
        partial_chunk: PartialEncodedChunk,
        shard_chunk: Option<ShardChunk>,
    ) {
        self.do_send(ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk });
    }
    fn saw_invalid_chunk(&self, chunk: EncodedShardChunk) {
        self.do_send(ShardsManagerResponse::InvalidChunk(chunk));
    }
    fn chunk_header_ready_for_inclusion(&self, chunk_header: ShardChunkHeader) {
        self.do_send(ShardsManagerResponse::ChunkHeaderReadyForInclusion(chunk_header));
    }
}

pub struct ShardedTransactionPool {
    tx_pools: HashMap<ShardId, TransactionPool>,

    /// Useful to make tests deterministic and reproducible,
    /// while keeping the security of randomization of transactions in pool
    rng_seed: RngSeed,
}

impl ShardedTransactionPool {
    pub fn new(rng_seed: RngSeed) -> Self {
        TransactionPool::init_metrics();
        Self { tx_pools: HashMap::new(), rng_seed }
    }

    pub fn get_pool_iterator(&mut self, shard_id: ShardId) -> Option<PoolIteratorWrapper<'_>> {
        self.tx_pools.get_mut(&shard_id).map(|pool| pool.pool_iterator())
    }

    /// Returns true if transaction is not in the pool before call
    pub fn insert_transaction(&mut self, shard_id: ShardId, tx: SignedTransaction) -> bool {
        self.pool_for_shard(shard_id).insert_transaction(tx)
    }

    pub fn remove_transactions(&mut self, shard_id: ShardId, transactions: &[SignedTransaction]) {
        if let Some(pool) = self.tx_pools.get_mut(&shard_id) {
            pool.remove_transactions(transactions)
        }
    }

    /// Computes a deterministic random seed for given `shard_id`.
    /// This seed is used to randomize the transaction pool.
    /// For better security we want the seed to different in each shard.
    /// For testing purposes we want it to be the reproducible and derived from the `self.rng_seed` and `shard_id`
    fn random_seed(base_seed: &RngSeed, shard_id: ShardId) -> RngSeed {
        let mut res = *base_seed;
        res[0] = shard_id as u8;
        res[1] = (shard_id / 256) as u8;
        res
    }

    fn pool_for_shard(&mut self, shard_id: ShardId) -> &mut TransactionPool {
        self.tx_pools
            .entry(shard_id)
            .or_insert_with(|| TransactionPool::new(Self::random_seed(&self.rng_seed, shard_id)))
    }

    pub fn reintroduce_transactions(
        &mut self,
        shard_id: ShardId,
        transactions: &[SignedTransaction],
    ) {
        self.pool_for_shard(shard_id).reintroduce_transactions(transactions.to_vec());
    }
}

pub trait ShardsManagerAdapter: Send + Sync + 'static {
    fn process_partial_encoded_chunk(&self, partial_encoded_chunk: PartialEncodedChunk);
    fn process_partial_encoded_chunk_forward(
        &self,
        partial_encoded_chunk_forward: PartialEncodedChunkForwardMsg,
    );
    fn process_partial_encoded_chunk_response(
        &self,
        partial_encoded_chunk_response: PartialEncodedChunkResponseMsg,
    );
    fn process_partial_encoded_chunk_request(
        &self,
        partial_encoded_chunk_request: PartialEncodedChunkRequestMsg,
        route_back: CryptoHash,
    );
    fn process_chunk_header_from_block(&self, chunk_header: &ShardChunkHeader);
    fn update_chain_head(&self, tip: Tip);
    fn distribute_encoded_chunk(
        &self,
        partial_chunk: PartialEncodedChunk,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        outgoing_receipts: Vec<Receipt>,
    );
    fn request_chunks(
        &self,
        chunks_to_request: Vec<ShardChunkHeader>,
        prev_hash: CryptoHash,
        header_head: Tip,
    );
    fn request_chunks_for_orphan(
        &self,
        chunks_to_request: Vec<ShardChunkHeader>,
        epoch_id: EpochId,
        ancestor_hash: CryptoHash,
        header_head: Tip,
    );
    fn check_incomplete_chunks(&self, prev_block_hash: CryptoHash);
}

#[derive(Message)]
#[rtype(result = "()")]
pub enum ShardsManagerRequest {
    ProcessPartialEncodedChunk(PartialEncodedChunk),
    ProcessPartialEncodedChunkForward(PartialEncodedChunkForwardMsg),
    ProcessPartialEncodedChunkResponse(PartialEncodedChunkResponseMsg),
    ProcessPartialEncodedChunkRequest {
        partial_encoded_chunk_request: PartialEncodedChunkRequestMsg,
        route_back: CryptoHash,
    },
    ProcessChunkHeaderFromBlock(ShardChunkHeader),
    UpdateChainHead(Tip),
    DistributeEncodedChunk {
        partial_chunk: PartialEncodedChunk,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        outgoing_receipts: Vec<Receipt>,
    },
    RequestChunks {
        chunks_to_request: Vec<ShardChunkHeader>,
        prev_hash: CryptoHash,
        header_head: Tip,
    },
    RequestChunksForOrphan {
        chunks_to_request: Vec<ShardChunkHeader>,
        epoch_id: EpochId,
        ancestor_hash: CryptoHash,
        header_head: Tip,
    },
    CheckIncompleteChunks(CryptoHash),
}

impl<A: MsgRecipient<ShardsManagerRequest>> ShardsManagerAdapter for A {
    fn process_partial_encoded_chunk(&self, partial_encoded_chunk: PartialEncodedChunk) {
        self.do_send(ShardsManagerRequest::ProcessPartialEncodedChunk(partial_encoded_chunk));
    }
    fn process_partial_encoded_chunk_forward(
        &self,
        partial_encoded_chunk_forward: PartialEncodedChunkForwardMsg,
    ) {
        self.do_send(ShardsManagerRequest::ProcessPartialEncodedChunkForward(
            partial_encoded_chunk_forward,
        ));
    }
    fn process_partial_encoded_chunk_response(
        &self,
        partial_encoded_chunk_response: PartialEncodedChunkResponseMsg,
    ) {
        self.do_send(ShardsManagerRequest::ProcessPartialEncodedChunkResponse(
            partial_encoded_chunk_response,
        ));
    }
    fn process_partial_encoded_chunk_request(
        &self,
        partial_encoded_chunk_request: PartialEncodedChunkRequestMsg,
        route_back: CryptoHash,
    ) {
        self.do_send(ShardsManagerRequest::ProcessPartialEncodedChunkRequest {
            partial_encoded_chunk_request,
            route_back,
        });
    }
    fn process_chunk_header_from_block(&self, chunk_header: &ShardChunkHeader) {
        self.do_send(ShardsManagerRequest::ProcessChunkHeaderFromBlock(chunk_header.clone()));
    }
    fn update_chain_head(&self, tip: Tip) {
        self.do_send(ShardsManagerRequest::UpdateChainHead(tip));
    }
    fn distribute_encoded_chunk(
        &self,
        partial_chunk: PartialEncodedChunk,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        outgoing_receipts: Vec<Receipt>,
    ) {
        self.do_send(ShardsManagerRequest::DistributeEncodedChunk {
            partial_chunk,
            encoded_chunk,
            merkle_paths,
            outgoing_receipts,
        });
    }
    fn request_chunks(
        &self,
        chunks_to_request: Vec<ShardChunkHeader>,
        prev_hash: CryptoHash,
        header_head: Tip,
    ) {
        self.do_send(ShardsManagerRequest::RequestChunks {
            chunks_to_request,
            prev_hash,
            header_head,
        });
    }
    fn request_chunks_for_orphan(
        &self,
        chunks_to_request: Vec<ShardChunkHeader>,
        epoch_id: EpochId,
        ancestor_hash: CryptoHash,
        header_head: Tip,
    ) {
        self.do_send(ShardsManagerRequest::RequestChunksForOrphan {
            chunks_to_request,
            epoch_id,
            ancestor_hash,
            header_head,
        });
    }
    fn check_incomplete_chunks(&self, prev_block_hash: CryptoHash) {
        self.do_send(ShardsManagerRequest::CheckIncompleteChunks(prev_block_hash));
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::epoch_manager::RngSeed;

    use crate::client::ShardedTransactionPool;

    const TEST_SEED: RngSeed = [3; 32];

    #[test]
    fn test_random_seed_with_shard_id() {
        let seed0 = ShardedTransactionPool::random_seed(&TEST_SEED, 0);
        let seed10 = ShardedTransactionPool::random_seed(&TEST_SEED, 10);
        let seed256 = ShardedTransactionPool::random_seed(&TEST_SEED, 256);
        let seed1000 = ShardedTransactionPool::random_seed(&TEST_SEED, 1000);
        let seed1000000 = ShardedTransactionPool::random_seed(&TEST_SEED, 1_000_000);
        assert_ne!(seed0, seed10);
        assert_ne!(seed0, seed256);
        assert_ne!(seed0, seed1000);
        assert_ne!(seed0, seed1000000);
        assert_ne!(seed10, seed256);
        assert_ne!(seed10, seed1000);
        assert_ne!(seed10, seed1000000);
        assert_ne!(seed256, seed1000);
        assert_ne!(seed256, seed1000000);
        assert_ne!(seed1000, seed1000000);
    }
}
