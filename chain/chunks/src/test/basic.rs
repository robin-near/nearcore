use std::sync::Arc;

use near_async::test_loop::CaptureEvents;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;

use crate::{
    adapter::ShardsManagerRequestFromClient,
    client::ShardsManagerResponse,
    test_loop::{ForwardClientRequestToShardsManager, ForwardNetworkRequestToShardsManager},
    test_utils::ChunkTestFixture,
    ShardsManager,
};

#[derive(derive_more::AsMut)]
struct TestData {
    #[as_mut]
    shards_manager: ShardsManager,
    #[as_mut]
    client_events: Vec<ShardsManagerResponse>,
}

#[derive(derive_more::TryInto, Debug, derive_more::From)]
enum TestEvent {
    ClientToShardsManager(ShardsManagerRequestFromClient),
    NetworkToShardsManager(ShardsManagerRequestFromNetwork),
    ShardsManagerToClient(ShardsManagerResponse),
}

type ShardsManagerTestLoopBuilder = near_async::test_loop::TestLoopBuilder<TestEvent>;

#[test]
fn test_basic() {
    let fixture = ChunkTestFixture::default(); // TODO: eventually remove
    let builder = ShardsManagerTestLoopBuilder::new();
    let shards_manager = ShardsManager::new(
        Some(fixture.mock_chunk_part_owner.clone()),
        fixture.mock_runtime.clone(), // TODO: make thinner
        fixture.mock_network.clone(), // TODO: replace with sender
        Arc::new(builder.sender()),
        fixture.chain_store.new_read_only_chunks_store(),
        fixture.mock_chain_head.clone(),
        fixture.mock_chain_head.clone(),
    );
    let test_data = TestData { shards_manager, client_events: vec![] };
    let mut test = builder.build(test_data);
    test.register_handler(Box::new(ForwardClientRequestToShardsManager {}));
    test.register_handler(Box::new(ForwardNetworkRequestToShardsManager {}));
    test.register_handler(Box::new(CaptureEvents::<ShardsManagerResponse>::new()));
    test.add_event(TestEvent::NetworkToShardsManager(
        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
            fixture.make_partial_encoded_chunk(&fixture.all_part_ords),
        ),
    ));
    test.run();
    assert_eq!(test.data.client_events.len(), 2);
    match &test.data.client_events[1] {
        ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk } => {
            assert_eq!(partial_chunk.parts().len(), fixture.all_part_ords.len());
            assert!(shard_chunk.is_some());
        }
        _ => panic!(),
    }
}
