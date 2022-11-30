use std::{collections::HashMap, sync::Arc, time::Instant};

use lru::LruCache;
use near_chain::{
    topology::Topology, Block, BlockHeader, Chain, ChainGenesis, ChainStore, ChainStoreAccess,
    RuntimeAdapter,
};
use near_chunks::{
    client::ShardedTransactionPool,
    logic::{cares_about_shard_this_or_next_epoch, decode_encoded_chunk, persist_chunk},
    ShardsManager,
};
use near_client_primitives::{debug::ChunkProduction, types::Error};
use near_epoch_manager::RngSeed;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_o11y::WithSpanContextExt;
use near_primitives::{
    challenge::{Challenge, ChallengeBody},
    hash::CryptoHash,
    merkle::{merklize, MerklePath},
    receipt::Receipt,
    sharding::{EncodedShardChunk, ReedSolomonWrapper, ShardChunkHeader},
    time::Clock,
    transaction::SignedTransaction,
    types::{chunk_extra::ChunkExtra, AccountId, BlockHeight, EpochId, NumBlocks, ShardId},
    validator_signer::ValidatorSigner,
};
use tracing::{debug, log::error};

use crate::{debug::PRODUCTION_TIMES_CACHE_SIZE, metrics};
pub struct Production {
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    validator_signer: Arc<dyn ValidatorSigner>,
    network_adapter: Arc<dyn PeerManagerAdapter>,
    transaction_validity_period: NumBlocks,
    rs: ReedSolomonWrapper,

    sharded_tx_pool: ShardedTransactionPool,
    /// List of currently accumulated challenges.
    pub challenges: HashMap<CryptoHash, Challenge>,

    /// Chunk production timing information. Used only for debug purposes.
    pub chunk_production_info: LruCache<(BlockHeight, ShardId), ChunkProduction>,
}

impl Production {
    pub fn new(
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        validator_signer: Arc<dyn ValidatorSigner>,
        network_adapter: Arc<dyn PeerManagerAdapter>,
        chain_genesis: &ChainGenesis,
        rng_seed: RngSeed,
    ) -> Self {
        let data_parts = runtime_adapter.num_data_parts();
        let parity_parts = runtime_adapter.num_total_parts() - data_parts;
        Self {
            runtime_adapter,
            validator_signer,
            network_adapter,
            transaction_validity_period: chain_genesis.transaction_validity_period,
            rs: ReedSolomonWrapper::new(data_parts, parity_parts),
            sharded_tx_pool: ShardedTransactionPool::new(rng_seed),
            challenges: HashMap::new(),
            chunk_production_info: LruCache::new(PRODUCTION_TIMES_CACHE_SIZE),
        }
    }

    pub fn me(&self) -> &AccountId {
        self.validator_signer.validator_id()
    }

    pub fn maybe_produce_new_chunks_on_top_of_block(
        &mut self,
        store: &mut ChainStore,
        shards_manager: &mut ShardsManager,
        block: &Block,
    ) -> Vec<ShardChunkHeader> {
        // Produce new chunks
        let epoch_id =
            self.runtime_adapter.get_epoch_id_from_prev_block(block.header().hash()).unwrap();
        let mut produced_chunk_headers = vec![];
        for shard_id in 0..self.runtime_adapter.num_shards(&epoch_id).unwrap() {
            let chunk_proposer = self
                .runtime_adapter
                .get_chunk_producer(&epoch_id, block.header().height() + 1, shard_id)
                .unwrap();

            if &chunk_proposer == self.me() {
                let _span = tracing::debug_span!(
                            target: "client",
                            "on_block_accepted_produce_chunk",
                            prev_block_hash = ?*block.hash(),
                            ?shard_id)
                .entered();
                let _timer = metrics::PRODUCE_AND_DISTRIBUTE_CHUNK_TIME
                    .with_label_values(&[&shard_id.to_string()])
                    .start_timer();
                match self.produce_chunk(
                    store,
                    *block.hash(),
                    &epoch_id,
                    Chain::get_prev_chunk_header(&*self.runtime_adapter, &block, shard_id).unwrap(),
                    block.header().height() + 1,
                    shard_id,
                ) {
                    Ok(Some((encoded_chunk, merkle_paths, receipts))) => {
                        produced_chunk_headers.push(encoded_chunk.cloned_header());
                        self.persist_and_distribute_encoded_chunk(
                            store,
                            shards_manager,
                            encoded_chunk,
                            merkle_paths,
                            receipts,
                        )
                        .expect("Failed to process produced chunk");
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!(target: "production", "Failed to produce chunk: {:?}", e);
                    }
                }
            }
        }
        produced_chunk_headers
    }

    pub fn produce_chunk(
        &mut self,
        store: &mut ChainStore,
        prev_block_hash: CryptoHash,
        epoch_id: &EpochId,
        last_header: ShardChunkHeader,
        next_height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<Option<(EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>)>, Error> {
        let timer = Instant::now();
        let _timer =
            metrics::PRODUCE_CHUNK_TIME.with_label_values(&[&shard_id.to_string()]).start_timer();
        let _span = tracing::debug_span!(target: "client", "produce_chunk", next_height, shard_id, ?epoch_id).entered();
        let validator_signer = self.validator_signer.clone();

        let chunk_proposer =
            self.runtime_adapter.get_chunk_producer(epoch_id, next_height, shard_id).unwrap();
        if validator_signer.validator_id() != &chunk_proposer {
            debug!(target: "client", "Not producing chunk for shard {}: chain at {}, not block producer for next block. Me: {}, proposer: {}", shard_id, next_height, validator_signer.validator_id(), chunk_proposer);
            return Ok(None);
        }

        if self.runtime_adapter.is_next_block_epoch_start(&prev_block_hash)? {
            let prev_prev_hash = *store.get_block_header(&prev_block_hash)?.prev_hash();
            if !store.prev_block_is_caught_up(&prev_prev_hash, &prev_block_hash)? {
                // See comment in similar snipped in `produce_block`
                debug!(target: "client", "Produce chunk: prev block is not caught up");
                return Err(Error::ChunkProducer(
                    "State for the epoch is not downloaded yet, skipping chunk production"
                        .to_string(),
                ));
            }
        }

        debug!(
            target: "client",
            "Producing chunk at height {} for shard {}, I'm {}",
            next_height,
            shard_id,
            validator_signer.validator_id()
        );

        let shard_uid = self.runtime_adapter.shard_id_to_uid(shard_id, epoch_id)?;
        let chunk_extra = store
            .get_chunk_extra(&prev_block_hash, &shard_uid)
            .map_err(|err| Error::ChunkProducer(format!("No chunk extra available: {}", err)))?;

        let prev_block_header = store.get_block_header(&prev_block_hash)?;
        let transactions =
            self.prepare_transactions(store, shard_id, &chunk_extra, &prev_block_header)?;
        let transactions = transactions;
        #[cfg(feature = "test_features")]
        let transactions = Self::maybe_insert_invalid_transaction(
            transactions,
            prev_block_hash,
            self.produce_invalid_tx_in_chunks,
        );
        let num_filtered_transactions = transactions.len();
        let (tx_root, _) = merklize(&transactions);
        let outgoing_receipts = store.get_outgoing_receipts_for_shard(
            self.runtime_adapter.as_ref(),
            prev_block_hash,
            shard_id,
            last_header.height_included(),
        )?;

        // Receipts proofs root is calculating here
        //
        // For each subset of incoming_receipts_into_shard_i_from_the_current_one
        // we calculate hash here and save it
        // and then hash all of them into a single receipts root
        //
        // We check validity in two ways:
        // 1. someone who cares about shard will download all the receipts
        // and checks that receipts_root equals to all receipts hashed
        // 2. anyone who just asks for one's incoming receipts
        // will receive a piece of incoming receipts only
        // with merkle receipts proofs which can be checked locally
        let shard_layout = self.runtime_adapter.get_shard_layout(epoch_id)?;
        let outgoing_receipts_hashes =
            Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout);
        let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);

        let protocol_version = self.runtime_adapter.get_epoch_protocol_version(epoch_id)?;
        let gas_used = chunk_extra.gas_used();
        #[cfg(feature = "test_features")]
        let gas_used = if self.produce_invalid_chunks { gas_used + 1 } else { gas_used };
        let (encoded_chunk, merkle_paths) = ShardsManager::create_encoded_shard_chunk(
            prev_block_hash,
            *chunk_extra.state_root(),
            *chunk_extra.outcome_root(),
            next_height,
            shard_id,
            gas_used,
            chunk_extra.gas_limit(),
            chunk_extra.balance_burnt(),
            chunk_extra.validator_proposals().collect(),
            transactions,
            &outgoing_receipts,
            outgoing_receipts_root,
            tx_root,
            &*validator_signer,
            &mut self.rs,
            protocol_version,
        )?;

        debug!(
            target: "client",
            height=next_height,
            shard_id,
            me=%validator_signer.validator_id(),
            chunk_hash=%encoded_chunk.chunk_hash().0,
            %prev_block_hash,
            "Produced chunk with {} txs and {} receipts",
            num_filtered_transactions,
            outgoing_receipts.len(),
        );

        metrics::CHUNK_PRODUCED_TOTAL.inc();
        self.chunk_production_info.put(
            (next_height, shard_id),
            ChunkProduction {
                chunk_production_time: Some(Clock::utc()),
                chunk_production_duration_millis: Some(timer.elapsed().as_millis() as u64),
            },
        );
        Ok(Some((encoded_chunk, merkle_paths, outgoing_receipts)))
    }

    /// Prepares an ordered list of valid transactions from the pool up the limits.
    fn prepare_transactions(
        &mut self,
        store: &mut ChainStore,
        shard_id: ShardId,
        chunk_extra: &ChunkExtra,
        prev_block_header: &BlockHeader,
    ) -> Result<Vec<SignedTransaction>, Error> {
        let next_epoch_id =
            self.runtime_adapter.get_epoch_id_from_prev_block(prev_block_header.hash())?;
        let protocol_version = self.runtime_adapter.get_epoch_protocol_version(&next_epoch_id)?;

        let transactions = if let Some(mut iter) = self.sharded_tx_pool.get_pool_iterator(shard_id)
        {
            let transaction_validity_period = self.transaction_validity_period;
            self.runtime_adapter.prepare_transactions(
                prev_block_header.gas_price(),
                chunk_extra.gas_limit(),
                &next_epoch_id,
                shard_id,
                *chunk_extra.state_root(),
                // while the height of the next block that includes the chunk might not be prev_height + 1,
                // passing it will result in a more conservative check and will not accidentally allow
                // invalid transactions to be included.
                prev_block_header.height() + 1,
                &mut iter,
                &mut |tx: &SignedTransaction| -> bool {
                    store
                        .check_transaction_validity_period(
                            prev_block_header,
                            &tx.transaction.block_hash,
                            transaction_validity_period,
                        )
                        .is_ok()
                },
                protocol_version,
            )?
        } else {
            vec![]
        };
        // Reintroduce valid transactions back to the pool. They will be removed when the chunk is
        // included into the block.
        self.sharded_tx_pool.reintroduce_transactions(shard_id, &transactions);
        Ok(transactions)
    }

    pub fn insert_transaction(&mut self, shard_id: ShardId, transaction: SignedTransaction) {
        self.sharded_tx_pool.insert_transaction(shard_id, transaction);
    }

    pub fn remove_transactions_for_block(&mut self, store: &mut ChainStore, block: &Block) {
        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if block.header().height() == chunk_header.height_included() {
                if cares_about_shard_this_or_next_epoch(
                    Some(self.me()),
                    block.header().prev_hash(),
                    shard_id,
                    true,
                    self.runtime_adapter.as_ref(),
                ) {
                    self.sharded_tx_pool.remove_transactions(
                        shard_id,
                        // By now the chunk must be in store, otherwise the block would have been orphaned
                        store.get_chunk(&chunk_header.chunk_hash()).unwrap().transactions(),
                    );
                }
            }
        }
        for challenge in block.challenges().iter() {
            self.challenges.remove(&challenge.hash);
        }
    }

    pub fn reintroduce_transactions_for_block(&mut self, store: &mut ChainStore, block: &Block) {
        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if block.header().height() == chunk_header.height_included() {
                if cares_about_shard_this_or_next_epoch(
                    Some(self.me()),
                    block.header().prev_hash(),
                    shard_id,
                    false,
                    self.runtime_adapter.as_ref(),
                ) {
                    self.sharded_tx_pool.reintroduce_transactions(
                        shard_id,
                        // By now the chunk must be in store, otherwise the block would have been orphaned
                        store.get_chunk(&chunk_header.chunk_hash()).unwrap().transactions(),
                    );
                }
            }
        }
        for challenge in block.challenges().iter() {
            self.challenges.insert(challenge.hash, challenge.clone());
        }
    }

    pub fn send_challenges(&mut self, challenges: Vec<ChallengeBody>) {
        for body in challenges {
            let challenge = Challenge::produce(body, self.validator_signer.as_ref());
            self.challenges.insert(challenge.hash, challenge.clone());
            self.network_adapter.do_send(
                PeerManagerMessageRequest::NetworkRequests(NetworkRequests::Challenge(challenge))
                    .with_span_context(),
            );
        }
    }

    pub fn persist_and_distribute_encoded_chunk(
        &mut self,
        store: &mut ChainStore,
        shards_manager: &mut ShardsManager,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        receipts: Vec<Receipt>,
    ) -> Result<(), Error> {
        let (shard_chunk, partial_chunk) = decode_encoded_chunk(
            &encoded_chunk,
            merkle_paths.clone(),
            Some(self.me()),
            self.runtime_adapter.as_ref(),
        )?;
        persist_chunk(partial_chunk.clone(), Some(shard_chunk), store)?;
        shards_manager.distribute_encoded_chunk(
            partial_chunk,
            encoded_chunk,
            &merkle_paths,
            receipts,
        )?;
        Ok(())
    }
}
