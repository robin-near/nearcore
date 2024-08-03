use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::test_utils::test_loop::ClientQueries;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::transactions::execute_money_transfers;
use crate::test_loop::utils::ONE_NEAR;
use near_async::messaging::CanSend;
use near_chain::ChainStoreAccess;
use near_client::SetNetworkInfo;
use near_network::types::{HighestHeightPeerInfo, NetworkInfo, PeerInfo};
use near_primitives::block::GenesisId;
use near_store::test_utils::create_test_store;
use tempfile::TempDir;

const NUM_CLIENTS: usize = 4;

#[test]
fn test_epoch_sync() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(10000)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        .transaction_validity_period(1000)
        .epoch_length(10)
        .validators_desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[])
        .shuffle_shard_assignment_for_chunk_producers(true);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } =
        builder.genesis(genesis.clone()).clients(clients).build();

    let first_epoch_tracked_shards = {
        let clients = node_datas
            .iter()
            .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
            .collect_vec();
        clients.tracked_shards_for_each_client()
    };
    tracing::info!("First epoch tracked shards: {:?}", first_epoch_tracked_shards);

    execute_money_transfers(&mut test_loop, &node_datas, &accounts);

    // Make sure the chain progresses for several epochs.
    let client_handle = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().height > 10050
        },
        Duration::seconds(50),
    );

    let clients = node_datas
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    let later_epoch_tracked_shards = clients.tracked_shards_for_each_client();
    tracing::info!("Later epoch tracked shards: {:?}", later_epoch_tracked_shards);
    assert_ne!(first_epoch_tracked_shards, later_epoch_tracked_shards);

    // Make a new TestLoopEnv, adding a new node to the network, and check that it can properly sync.
    let mut stores = Vec::new();
    for data in &node_datas {
        stores.push((
            test_loop
                .data
                .get(&data.client_sender.actor_handle())
                .client
                .chain
                .chain_store
                .store()
                .clone(),
            None,
        ));
    }
    stores.push((create_test_store(), None));

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    TestLoopEnv {
        test_loop,
        datas: node_datas,
        tempdir: TempDir::new().unwrap(), /* don't destroy yet */
    }
    .shutdown_and_drain_remaining_events(Duration::seconds(20));

    tracing::info!("Starting new TestLoopEnv with new node");

    let clients = accounts.iter().take(NUM_CLIENTS + 1).cloned().collect_vec();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = TestLoopBuilder::new()
        .genesis(genesis.clone())
        .clients(clients)
        .stores_override(stores)
        .tempdir_override(tempdir)
        .config_modifier(|config, _| {
            config.epoch_sync.epoch_sync_horizon = 30;
            config.epoch_sync.epoch_sync_accept_proof_max_horizon = 20;
        })
        .skip_warmup()
        .build();

    let chain0 = &test_loop.data.get(&node_datas[0].client_sender.actor_handle()).client.chain;
    let peer_info = HighestHeightPeerInfo {
        archival: false,
        genesis_id: GenesisId {
            chain_id: genesis.config.chain_id.clone(),
            hash: *chain0.genesis().hash(),
        },
        highest_block_hash: chain0.head().unwrap().last_block_hash,
        highest_block_height: chain0.head().unwrap().height,
        tracked_shards: vec![],
        peer_info: PeerInfo {
            account_id: Some(accounts[0].clone()),
            addr: None,
            id: node_datas[0].peer_id.clone(),
        },
    };
    node_datas[NUM_CLIENTS].client_sender.send(SetNetworkInfo(NetworkInfo {
        connected_peers: Vec::new(),
        highest_height_peers: vec![peer_info],
        known_producers: vec![],
        num_connected_peers: 0,
        peer_max_count: 0,
        received_bytes_per_sec: 0,
        sent_bytes_per_sec: 0,
        tier1_accounts_data: Vec::new(),
        tier1_accounts_keys: Vec::new(),
        tier1_connections: Vec::new(),
    }));

    let new_node = node_datas.last().unwrap().client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data| test_loop_data.get(&new_node).client.chain.head().unwrap().height > 10050,
        Duration::seconds(20),
    );
    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
