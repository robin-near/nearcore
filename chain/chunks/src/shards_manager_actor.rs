use std::{sync::Arc, time::Duration};

use actix::{Actor, Addr, Arbiter, ArbiterHandle, Context, Handler};
use near_chain::{chunks_store::ReadOnlyChunksStore, types::Tip, RuntimeAdapter};
use near_network::types::PeerManagerAdapter;
use near_primitives::types::AccountId;
use near_store::{DBCol, Store, HEAD_KEY};
use tracing::log::warn;

use crate::{
    client::{ClientAdapterForShardsManager, ShardsManagerRequest},
    ShardsManager,
};

pub struct ShardsManagerActor {
    shards_mgr: ShardsManager,
    chunk_request_retry_period: Duration,
}

impl ShardsManagerActor {
    fn new(shards_mgr: ShardsManager, chunk_request_retry_period: Duration) -> Self {
        Self { shards_mgr, chunk_request_retry_period }
    }

    fn periodically_resend_chunk_requests(&mut self, ctx: &mut Context<Self>) {
        self.shards_mgr.resend_chunk_requests();

        near_performance_metrics::actix::run_later(
            ctx,
            self.chunk_request_retry_period,
            move |act, ctx| {
                act.periodically_resend_chunk_requests(ctx);
            },
        );
    }
}

impl Actor for ShardsManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.periodically_resend_chunk_requests(ctx);
    }
}

impl Handler<ShardsManagerRequest> for ShardsManagerActor {
    type Result = ();

    fn handle(&mut self, msg: ShardsManagerRequest, _ctx: &mut Context<Self>) {
        match msg {
            ShardsManagerRequest::ProcessPartialEncodedChunk(partial_encoded_chunk) => {
                if let Err(e) =
                    self.shards_mgr.process_partial_encoded_chunk(partial_encoded_chunk.into())
                {
                    warn!(target: "chunks", "Error processing partial encoded chunk: {:?}", e);
                }
            }
            ShardsManagerRequest::ProcessPartialEncodedChunkForward(
                partial_encoded_chunk_forward,
            ) => {
                if let Err(e) = self
                    .shards_mgr
                    .process_partial_encoded_chunk_forward(partial_encoded_chunk_forward)
                {
                    warn!(target: "chunks", "Error processing partial encoded chunk forward: {:?}", e);
                }
            }
            ShardsManagerRequest::ProcessPartialEncodedChunkResponse(
                partial_encoded_chunk_response,
            ) => {
                if let Err(e) = self
                    .shards_mgr
                    .process_partial_encoded_chunk_response(partial_encoded_chunk_response)
                {
                    warn!(target: "chunks", "Error processing partial encoded chunk response: {:?}", e);
                }
            }
            ShardsManagerRequest::ProcessPartialEncodedChunkRequest {
                partial_encoded_chunk_request,
                route_back,
            } => {
                self.shards_mgr.process_partial_encoded_chunk_request(
                    partial_encoded_chunk_request,
                    route_back,
                );
            }
            ShardsManagerRequest::ProcessChunkHeaderFromBlock(chunk_header) => {
                if let Err(e) = self.shards_mgr.process_chunk_header_from_block(&chunk_header) {
                    warn!(target: "chunks", "Error processing chunk header from block: {:?}", e);
                }
            }
            ShardsManagerRequest::UpdateChainHead(tip) => self.shards_mgr.update_chain_head(tip),
            ShardsManagerRequest::DistributeEncodedChunk {
                partial_chunk,
                encoded_chunk,
                merkle_paths,
                outgoing_receipts,
            } => {
                if let Err(e) = self.shards_mgr.distribute_encoded_chunk(
                    partial_chunk,
                    encoded_chunk,
                    &merkle_paths,
                    outgoing_receipts,
                ) {
                    warn!(target: "chunks", "Error distributing encoded chunk: {:?}", e);
                }
            }
            ShardsManagerRequest::RequestChunks { chunks_to_request, prev_hash, header_head } => {
                self.shards_mgr.request_chunks(chunks_to_request, prev_hash, &header_head)
            }
            ShardsManagerRequest::RequestChunksForOrphan {
                chunks_to_request,
                epoch_id,
                ancestor_hash,
                header_head,
            } => self.shards_mgr.request_chunks_for_orphan(
                chunks_to_request,
                &epoch_id,
                ancestor_hash,
                &header_head,
            ),
            ShardsManagerRequest::CheckIncompleteChunks(prev_block_hash) => {
                self.shards_mgr.check_incomplete_chunks(&prev_block_hash)
            }
        }
    }
}

pub fn start_shards_manager(
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    network_adapter: Arc<dyn PeerManagerAdapter>,
    client_adapter_for_shards_manager: Arc<dyn ClientAdapterForShardsManager>,
    me: Option<AccountId>,
    store: Store,
    chunk_request_retry_period: Duration,
) -> (Addr<ShardsManagerActor>, ArbiterHandle) {
    let shards_manager_arbiter = Arbiter::new();
    let shards_manager_arbiter_handle = shards_manager_arbiter.handle();
    let chain_head = store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY).ok().flatten();
    let chunks_store = ReadOnlyChunksStore::new(store);
    let shards_manager = ShardsManager::new(
        me,
        runtime_adapter,
        network_adapter,
        client_adapter_for_shards_manager,
        chunks_store,
        chain_head,
    );
    let shards_manager_addr =
        ShardsManagerActor::start_in_arbiter(&shards_manager_arbiter_handle, move |_| {
            ShardsManagerActor::new(shards_manager, chunk_request_retry_period)
        });
    (shards_manager_addr, shards_manager_arbiter_handle)
}
