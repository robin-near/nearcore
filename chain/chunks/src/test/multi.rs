use std::{str::FromStr, sync::Arc};

use near_async::{
    messaging::Sender,
    test_loop::{CaptureEvents, LoopEventHandler},
};
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork,
    types::{PeerManagerMessageRequest, SetChainInfo},
};
use near_primitives::types::AccountId;

use crate::{
    adapter::ShardsManagerRequestFromClient,
    client::ShardsManagerResponse,
    test_loop::{
        ForwardClientRequestToShardsManager, ForwardNetworkRequestToShardsManager,
        RouteShardsManagerNetworkMessages,
    },
    test_utils::ChunkTestFixture,
    ShardsManager,
};

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct TestData {
    #[as_mut]
    shards_manager: ShardsManager,
    #[as_mut]
    client_events: Vec<ShardsManagerResponse>,
    #[as_ref]
    account_id: AccountId,
}

#[derive(derive_more::TryInto, Debug, derive_more::From)]
enum TestEvent {
    ClientToShardsManager(ShardsManagerRequestFromClient),
    NetworkToShardsManager(ShardsManagerRequestFromNetwork),
    ShardsManagerToClient(ShardsManagerResponse),
    OutboundNetwork(PeerManagerMessageRequest),
    ChainInfoForNetwork(SetChainInfo),
}

type ShardsManagerTestLoopBuilder = near_async::test_loop::TestLoopBuilder<(usize, TestEvent)>;

#[test]
fn test_multi() {
    let builder = ShardsManagerTestLoopBuilder::new();
    let data = (0..27)
        .map(|idx| {
            let fixture = ChunkTestFixture::default(); // TODO: eventually remove
            let shards_manager = ShardsManager::new(
                Some(fixture.mock_chunk_part_owner.clone()),
                fixture.mock_runtime.clone(), // TODO: make thinner
                builder.sender_for_index(idx),
                builder.sender_for_index(idx),
                fixture.chain_store.new_read_only_chunks_store(),
                fixture.mock_chain_head.clone(),
                fixture.mock_chain_head.clone(),
            );
            TestData {
                shards_manager,
                client_events: vec![],
                account_id: AccountId::from_str(&if idx == 0 {
                    "test".to_string()
                } else {
                    format!("test_{}", ('a'..='z').skip(idx - 1).next().unwrap())
                })
                .unwrap(),
            }
        })
        .collect::<Vec<_>>();
    let sender = builder.sender();
    let mut test = builder.build(data);
    test.register_handler(ForwardClientRequestToShardsManager.indexed());
    test.register_handler(ForwardNetworkRequestToShardsManager.indexed());
    test.register_handler(CaptureEvents::<ShardsManagerResponse>::new().indexed());
    test.register_handler(RouteShardsManagerNetworkMessages::new(sender.clone()));

    let fixture = ChunkTestFixture::default(); // TODO: eventually remove
    sender.send((
        0,
        ShardsManagerRequestFromClient::DistributeEncodedChunk {
            partial_chunk: fixture.make_partial_encoded_chunk(&fixture.all_part_ords),
            encoded_chunk: fixture.mock_encoded_chunk.clone(),
            merkle_paths: fixture.mock_merkle_paths.clone(),
            outgoing_receipts: fixture.mock_outgoing_receipts.clone(),
        }
        .into(),
    ));
    test.run();
}
