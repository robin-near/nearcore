use std::{str::FromStr, time::Duration};

use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::{
    messaging::{CanSend, IntoSender},
    test_loop::{
        event_handler::{capture_events, periodic_timer, LoopEventHandlerHelpers},
        futures::MessageExpectingResponse,
    },
};
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork,
    types::{PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo},
};
use near_primitives::types::AccountId;

use crate::{
    adapter::ShardsManagerRequestFromClient,
    client::ShardsManagerResponse,
    test_loop::{
        ForwardClientRequestToShardsManager, ForwardNetworkRequestToShardsManager,
        RouteShardsManagerNetworkMessages, ShardsManagerResendRequests,
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

#[derive(EnumTryInto, Debug, EnumFrom)]
enum TestEvent {
    ClientToShardsManager(ShardsManagerRequestFromClient),
    NetworkToShardsManager(ShardsManagerRequestFromNetwork),
    ShardsManagerResendRequests(ShardsManagerResendRequests),
    ShardsManagerToClient(ShardsManagerResponse),
    OutboundNetwork(PeerManagerMessageRequest),
    OutboundNetworkAsync(
        MessageExpectingResponse<PeerManagerMessageRequest, Result<PeerManagerMessageResponse, ()>>,
    ),
    ChainInfoForNetwork(SetChainInfo),
}

type ShardsManagerTestLoopBuilder = near_async::test_loop::TestLoopBuilder<(usize, TestEvent)>;

#[test]
fn test_multi() {
    let builder = ShardsManagerTestLoopBuilder::new();
    let data = (0..13)
        .map(|idx| {
            let fixture = ChunkTestFixture::default(); // TODO: eventually remove
            let shards_manager = ShardsManager::new(
                Some(fixture.mock_chunk_part_owner.clone()),
                fixture.mock_runtime.clone(), // TODO: make thinner
                builder.sender().for_index(idx).into_sender(),
                builder.sender().for_index(idx).into_sender(),
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
    for idx in 0..test.data.len() {
        test.register_handler(ForwardClientRequestToShardsManager.widen().for_index(idx));
        test.register_handler(ForwardNetworkRequestToShardsManager.widen().for_index(idx));
        test.register_handler(capture_events::<ShardsManagerResponse>().widen().for_index(idx));
        test.register_handler(RouteShardsManagerNetworkMessages::new(Duration::from_millis(10)));
        test.register_handler(
            periodic_timer(
                Duration::from_millis(400),
                ShardsManagerResendRequests,
                |data: &mut ShardsManager| data.resend_chunk_requests(),
            )
            .widen()
            .for_index(idx),
        )
    }

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
    test.run(Duration::from_secs(1));
}
