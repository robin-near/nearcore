use near_network::shards_manager::ShardsManagerRequestFromNetwork;

use crate::{adapter::ShardsManagerRequestFromClient, test_utils::ChunkTestFixture, ShardsManager};

#[derive(derive_more::AsMut)]
struct TestData {
    #[as_mut]
    shards_manager: ShardsManager,
}

#[derive(derive_more::TryInto, Debug)]
enum TestEvent {
    ClientRequest(ShardsManagerRequestFromClient),
    NetworkRequest(ShardsManagerRequestFromNetwork),
}

type ShardsManagerTestLoop = near_async::test_loop::TestLoop<TestData, TestEvent>;

#[test]
fn test_basic() {
    use crate::test_loop::{
        ForwardClientRequestToShardsManager, ForwardNetworkRequestToShardsManager,
    };

    let fixture = ChunkTestFixture::default();
    let shards_manager = ShardsManager::new(
        Some(fixture.mock_chunk_part_owner.clone()),
        fixture.mock_runtime.clone(),
        fixture.mock_network.clone(),
        fixture.mock_client_adapter.clone(),
        fixture.chain_store.new_read_only_chunks_store(),
        fixture.mock_chain_head.clone(),
        fixture.mock_chain_head.clone(),
    );
    let test_data = TestData { shards_manager };
    let mut test_loop = ShardsManagerTestLoop::new(test_data);
    test_loop.register_handler(Box::new(ForwardClientRequestToShardsManager {}));
    test_loop.register_handler(Box::new(ForwardNetworkRequestToShardsManager {}));
    test_loop.add_event(TestEvent::NetworkRequest(
        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
            fixture.make_partial_encoded_chunk(&fixture.all_part_ords),
        ),
    ));
    test_loop.run();
}
