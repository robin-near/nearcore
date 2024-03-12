use assert_matches::assert_matches;
use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::messaging::{noop, IntoMultiSender, IntoSender, MessageWithCallback, SendAsync};
use near_async::test_loop::adhoc::{handle_adhoc_events, AdhocEvent, AdhocEventSender};
use near_async::test_loop::event_handler::ignore_events;
use near_async::test_loop::futures::{
    drive_async_computations, drive_delayed_action_runners, drive_futures,
    TestLoopAsyncComputationEvent, TestLoopDelayedActionEvent, TestLoopTask,
};
use near_async::test_loop::TestLoopBuilder;
use near_async::time::Duration;
use near_chain::chunks_store::ReadOnlyChunksStore;
use near_chain::ChainGenesis;
use near_chain_configs::{ClientConfig, Genesis, GenesisConfig, GenesisRecords};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::client::ShardsManagerResponse;
use near_chunks::test_loop::{
    forward_client_request_to_shards_manager, forward_network_request_to_shards_manager,
    route_shards_manager_network_messages,
};
use near_chunks::ShardsManager;
use near_client::client_actions::{
    ClientActions, ClientSenderForClientMessage, SyncJobsSenderForClientMessage,
};
use near_client::sync_jobs_actions::{
    ClientSenderForSyncJobsMessage, SyncJobsActions, SyncJobsSenderForSyncJobsMessage,
};
use near_client::test_utils::client_actions_test_utils::{
    forward_client_messages_from_client_to_client_actions,
    forward_client_messages_from_network_to_client_actions,
    forward_client_messages_from_shards_manager,
    forward_client_messages_from_sync_jobs_to_client_actions,
};
use near_client::test_utils::sync_jobs_test_utils::forward_sync_jobs_messages_from_client_to_sync_jobs_actions;
use near_client::test_utils::test_loop::{route_network_messages_to_client, ClientQueries};
use near_client::{Client, SyncAdapter, SyncMessage};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_network::client::{
    ClientSenderForNetwork, ClientSenderForNetworkMessage, ProcessTxRequest,
};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::types::{PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo};
use near_primitives::network::PeerId;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, AccountInfo};
use near_primitives::utils::from_timestamp;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::FinalExecutionStatus;
use near_primitives_core::account::{AccessKey, Account};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::NumSeats;
use near_store::config::StateSnapshotType;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use near_store::TrieConfig;
use near_test_contracts::ft_contract;
use nearcore::NightshadeRuntime;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct TestData {
    pub dummy: (),
    pub account: AccountId,
    pub client: ClientActions,
    pub sync_jobs: SyncJobsActions,
    pub shards_manager: ShardsManager,
}

impl AsMut<TestData> for TestData {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

impl AsRef<Client> for TestData {
    fn as_ref(&self) -> &Client {
        &self.client.client
    }
}

#[derive(EnumTryInto, Debug, EnumFrom)]
#[allow(clippy::large_enum_variant)]
enum TestEvent {
    /// Allows futures to be spawn and executed.
    Task(Arc<TestLoopTask>),
    /// Allows adhoc events to be used for the test (only used inside this file).
    Adhoc(AdhocEvent<TestData>),
    /// Allows asynchronous computation (chunk application, stateless validation, etc.).
    AsyncComputation(TestLoopAsyncComputationEvent),

    /// Allows delayed actions to be posted, as if ClientActor scheduled them, e.g. timers.
    ClientDelayedActions(TestLoopDelayedActionEvent<ClientActions>),
    /// Allows delayed actions to be posted, as if ShardsManagerActor scheduled them, e.g. timers.
    ShardsManagerDelayedActions(TestLoopDelayedActionEvent<ShardsManager>),

    /// Message that the network layer sends to the client.
    ClientEventFromNetwork(ClientSenderForNetworkMessage),
    /// Message that the client sends to the client itself.
    ClientEventFromClient(ClientSenderForClientMessage),
    /// Message that the SyncJobs component sends to the client.
    ClientEventFromSyncJobs(ClientSenderForSyncJobsMessage),
    /// Message that the ShardsManager component sends to the client.
    ClientEventFromShardsManager(ShardsManagerResponse),
    /// Message that the state sync adapter sends to the client.
    ClientEventFromStateSyncAdapter(SyncMessage),

    /// Message that the client sends to the SyncJobs component.
    SyncJobsEventFromClient(SyncJobsSenderForClientMessage),
    /// Message that the SyncJobs component sends to itself.
    SyncJobsEventFromSyncJobs(SyncJobsSenderForSyncJobsMessage),

    /// Message that the client sends to the ShardsManager component.
    ShardsManagerRequestFromClient(ShardsManagerRequestFromClient),
    /// Message that the network layer sends to the ShardsManager component.
    ShardsManagerRequestFromNetwork(ShardsManagerRequestFromNetwork),

    /// Outgoing network message that is sent by any of the components of this node.
    OutgoingNetworkMessage(PeerManagerMessageRequest),
    /// Same as OutgoingNetworkMessage, but of the variant that requests a response.
    OutgoingNetworkMessageForResult(
        MessageWithCallback<PeerManagerMessageRequest, PeerManagerMessageResponse>,
    ),
    /// Calls to the network component to set chain info.
    SetChainInfo(SetChainInfo),
}

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

#[test]
fn test_client_with_multi_test_loop() {
    const NUM_CLIENTS: usize = 4;
    const NETWORK_DELAY: Duration = Duration::milliseconds(10);
    let builder = TestLoopBuilder::<(usize, TestEvent)>::new();

    let validator_stake = 1000000 * ONE_NEAR;
    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();

    // TODO: Make some builder for genesis.
    let mut genesis_config = GenesisConfig {
        genesis_time: from_timestamp(builder.clock().now_utc().unix_timestamp_nanos() as u64),
        protocol_version: PROTOCOL_VERSION,
        genesis_height: 10000,
        shard_layout: ShardLayout::v1(
            vec!["account3", "account5", "account7"]
                .into_iter()
                .map(|a| a.parse().unwrap())
                .collect(),
            None,
            1,
        ),
        min_gas_price: 0,
        max_gas_price: 0,
        gas_limit: 100000000000000,
        transaction_validity_period: 1000,
        validators: (0..NUM_CLIENTS)
            .map(|idx| AccountInfo {
                account_id: accounts[idx].clone(),
                amount: validator_stake,
                public_key: create_test_signer(accounts[idx].as_str()).public_key(),
            })
            .collect(),
        epoch_length: 10,
        protocol_treasury_account: accounts[NUM_CLIENTS].clone(),
        num_block_producer_seats: 4,
        minimum_validators_per_shard: 1,
        num_block_producer_seats_per_shard: vec![4, 4, 4, 4],
        ..Default::default()
    };
    let mut records = Vec::new();
    for (i, account) in accounts.iter().enumerate() {
        // The staked amount must be consistent with validators from genesis.
        let staked = if i < NUM_CLIENTS { validator_stake } else { 0 };
        records.push(StateRecord::Account {
            account_id: account.clone(),
            account: Account::new(
                initial_balance,
                staked,
                0,
                CryptoHash::default(),
                0,
                PROTOCOL_VERSION,
            ),
        });
        records.push(StateRecord::AccessKey {
            account_id: account.clone(),
            public_key: create_user_test_signer(&account).public_key,
            access_key: AccessKey::full_access(),
        });
        // The total supply must be correct to pass validation.
        genesis_config.total_supply += initial_balance + staked;
    }
    let genesis = Genesis::new(genesis_config, GenesisRecords(records)).unwrap();

    let mut datas = Vec::new();
    for idx in 0..NUM_CLIENTS {
        let mut client_config = ClientConfig::test(true, 600, 2000, 4, false, true, false, false);
        client_config.max_block_wait_delay = Duration::seconds(6);
        let store = create_test_store();
        initialize_genesis_state(store.clone(), &genesis, None);

        let sync_jobs_actions = SyncJobsActions::new(
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<ClientSenderForSyncJobsMessage, _>(),
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<SyncJobsSenderForSyncJobsMessage, _>(),
        );
        let chain_genesis = ChainGenesis::new(&genesis.config);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let shard_tracker = ShardTracker::new(TrackedConfig::AllShards, epoch_manager.clone());
        let state_sync_adapter = Arc::new(RwLock::new(SyncAdapter::new(
            builder.sender().for_index(idx).into_sender(),
            builder.sender().for_index(idx).into_sender(),
        )));
        let runtime_adapter = NightshadeRuntime::test_with_trie_config(
            Path::new("."),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
            TrieConfig { load_mem_tries_for_all_shards: true, ..Default::default() },
            StateSnapshotType::ForReshardingOnly,
        );

        let client = Client::new(
            builder.clock(),
            client_config.clone(),
            chain_genesis,
            epoch_manager.clone(),
            shard_tracker.clone(),
            state_sync_adapter,
            runtime_adapter,
            builder.sender().for_index(idx).into_multi_sender(),
            builder.sender().for_index(idx).into_sender(),
            Some(Arc::new(create_test_signer(accounts[idx].as_str()))),
            true,
            [0; 32],
            None,
            Arc::new(
                builder
                    .sender()
                    .for_index(idx)
                    .into_async_computation_spawner(|_| Duration::milliseconds(80)),
            ),
        )
        .unwrap();

        let shards_manager = ShardsManager::new(
            builder.clock(),
            Some(accounts[idx].clone()),
            epoch_manager,
            shard_tracker,
            builder.sender().for_index(idx).into_sender(),
            builder.sender().for_index(idx).into_sender(),
            ReadOnlyChunksStore::new(store),
            client.chain.head().unwrap(),
            client.chain.header_head().unwrap(),
        );

        let client_actions = ClientActions::new(
            builder.clock(),
            client,
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<ClientSenderForClientMessage, _>(),
            client_config,
            PeerId::random(),
            builder.sender().for_index(idx).into_multi_sender(),
            None,
            noop().into_sender(),
            None,
            Default::default(),
            None,
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<SyncJobsSenderForClientMessage, _>(),
            Box::new(builder.sender().for_index(idx).into_future_spawner()),
        )
        .unwrap();

        let data = TestData {
            dummy: (),
            account: accounts[idx].clone(),
            client: client_actions,
            sync_jobs: sync_jobs_actions,
            shards_manager,
        };
        datas.push(data);
    }

    let mut test = builder.build(datas);
    for idx in 0..NUM_CLIENTS {
        // Futures, adhoc events, async computations.
        test.register_handler(drive_futures().widen().for_index(idx));
        test.register_handler(handle_adhoc_events::<TestData>().widen().for_index(idx));
        test.register_handler(drive_async_computations().widen().for_index(idx));

        // Delayed actions.
        test.register_handler(
            drive_delayed_action_runners::<ClientActions>().widen().for_index(idx),
        );
        test.register_handler(
            drive_delayed_action_runners::<ShardsManager>().widen().for_index(idx),
        );

        // Messages to the client.
        test.register_handler(
            forward_client_messages_from_network_to_client_actions().widen().for_index(idx),
        );
        test.register_handler(
            forward_client_messages_from_client_to_client_actions().widen().for_index(idx),
        );
        test.register_handler(
            forward_client_messages_from_sync_jobs_to_client_actions().widen().for_index(idx),
        );
        test.register_handler(forward_client_messages_from_shards_manager().widen().for_index(idx));
        // TODO: handle state sync adapter -> client.

        // Messages to the SyncJobs component.
        test.register_handler(
            forward_sync_jobs_messages_from_client_to_sync_jobs_actions(
                test.sender().for_index(idx).into_future_spawner(),
            )
            .widen()
            .for_index(idx),
        );
        // TODO: handle SyncJobs -> SyncJobs.

        // Messages to the ShardsManager component.
        test.register_handler(forward_client_request_to_shards_manager().widen().for_index(idx));
        test.register_handler(forward_network_request_to_shards_manager().widen().for_index(idx));

        // Messages to the network layer; multi-node messages are handled below.
        test.register_handler(ignore_events::<SetChainInfo>().widen().for_index(idx));
    }
    // Handles network routing. Outgoing messages are handled by emitting incoming messages to the
    // appropriate component of the appropriate node index.
    test.register_handler(route_network_messages_to_client(NETWORK_DELAY));
    test.register_handler(route_shards_manager_network_messages(NETWORK_DELAY));

    // Bootstrap the test by starting the components.
    // We use adhoc events for these, just so that the visualizer can see these as events rather
    // than happening outside of the TestLoop framework. Other than that, we could also just remove
    // the send_adhoc_event part and the test would still work.
    for idx in 0..NUM_CLIENTS {
        let sender = test.sender().for_index(idx);
        test.sender().for_index(idx).send_adhoc_event("start_client", move |data| {
            data.client.start(&mut sender.into_delayed_action_runner());
        });

        let sender = test.sender().for_index(idx);
        test.sender().for_index(idx).send_adhoc_event("start_shards_manager", move |data| {
            data.shards_manager.periodically_resend_chunk_requests(
                &mut sender.into_delayed_action_runner(),
                Duration::milliseconds(100),
            );
        })
    }

    // Give it some condition to stop running at. Here we run the test until the first client
    // reaches height 10003, with a timeout of 5sec (failing if it doesn't reach 10003 in time).
    test.run_until(
        |data| data[0].client.client.chain.head().unwrap().height == 10003,
        Duration::seconds(5),
    );
    for idx in 0..NUM_CLIENTS {
        test.sender().for_index(idx).send_adhoc_event("assertions", |data| {
            let chain = &data.client.client.chain;
            let block = chain.get_block_by_height(10002).unwrap();
            assert_eq!(
                block.header().chunk_mask(),
                &(0..NUM_CLIENTS).map(|_| true).collect::<Vec<_>>()
            );
        })
    }
    test.run_instant();

    let mut balances = accounts
        .iter()
        .cloned()
        .map(|account| (account, initial_balance))
        .collect::<HashMap<_, _>>();
    for i in 0..accounts.len() {
        let amount = ONE_NEAR * (i as u128 + 1);
        let tx = SignedTransaction::send_money(
            1,
            accounts[i].clone(),
            accounts[(i + 1) % accounts.len()].clone(),
            &create_user_test_signer(&accounts[i]),
            amount,
            *test.data[0].client.client.chain.get_block_by_height(10002).unwrap().hash(),
        );
        *balances.get_mut(&accounts[i]).unwrap() -= amount;
        *balances.get_mut(&accounts[(i + 1) % accounts.len()]).unwrap() += amount;
        drop(
            test.sender()
                .for_index(i % NUM_CLIENTS)
                .into_wrapped_multi_sender::<ClientSenderForNetworkMessage, ClientSenderForNetwork>(
                )
                .send_async(ProcessTxRequest {
                    transaction: tx,
                    is_forwarded: false,
                    check_only: false,
                }),
        );
    }
    test.run_until(
        |data| data[0].client.client.chain.head().unwrap().height == 10008,
        Duration::seconds(8),
    );

    for account in &accounts {
        assert_eq!(
            test.data.query_balance(account),
            *balances.get(account).unwrap(),
            "Account balance mismatch for account {}",
            account
        );
    }

    // Give the test a chance to finish off remaining important events in the event loop, which can
    // be important for properly shutting down the nodes.
    test.finish_remaining_events(Duration::seconds(1));
}

#[test]
fn test_repro_in_memory_trie_bug() {
    const NUM_CLIENTS: usize = 2;
    const NETWORK_DELAY: Duration = Duration::milliseconds(10);
    let builder = TestLoopBuilder::<(usize, TestEvent)>::new();

    let validator_stake = 1000000 * ONE_NEAR;
    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..20).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();

    // TODO: Make some builder for genesis.
    let mut genesis_config = GenesisConfig {
        genesis_time: from_timestamp(builder.clock().now_utc().unix_timestamp_nanos() as u64),
        protocol_version: PROTOCOL_VERSION,
        genesis_height: 10000,
        shard_layout: ShardLayout::v0_single_shard(),
        min_gas_price: 0,
        max_gas_price: 0,
        gas_limit: 100000000000000,
        transaction_validity_period: 1000,
        validators: (0..NUM_CLIENTS)
            .map(|idx| AccountInfo {
                account_id: accounts[idx].clone(),
                amount: validator_stake,
                public_key: create_test_signer(accounts[idx].as_str()).public_key(),
            })
            .collect(),
        epoch_length: 10,
        protocol_treasury_account: accounts[NUM_CLIENTS].clone(),
        num_block_producer_seats: NUM_CLIENTS as NumSeats,
        minimum_validators_per_shard: 1,
        num_block_producer_seats_per_shard: vec![NUM_CLIENTS as NumSeats],
        ..Default::default()
    };
    let mut records = Vec::new();
    for (i, account) in accounts.iter().enumerate() {
        // The staked amount must be consistent with validators from genesis.
        let staked = if i < NUM_CLIENTS { validator_stake } else { 0 };
        records.push(StateRecord::Account {
            account_id: account.clone(),
            account: Account::new(
                initial_balance,
                staked,
                0,
                CryptoHash::default(),
                0,
                PROTOCOL_VERSION,
            ),
        });
        records.push(StateRecord::AccessKey {
            account_id: account.clone(),
            public_key: create_user_test_signer(&account).public_key,
            access_key: AccessKey::full_access(),
        });
        // The total supply must be correct to pass validation.
        genesis_config.total_supply += initial_balance + staked;
    }

    let registrar_account = "registrar".parse::<AccountId>().unwrap();
    records.push(StateRecord::Account {
        account_id: registrar_account.clone(),
        account: Account::new(initial_balance, 0, 0, CryptoHash::default(), 0, PROTOCOL_VERSION),
    });
    records.push(StateRecord::AccessKey {
        account_id: registrar_account.clone(),
        public_key: create_user_test_signer(&registrar_account).public_key,
        access_key: AccessKey::full_access(),
    });
    genesis_config.total_supply += initial_balance;

    let genesis = Genesis::new(genesis_config, GenesisRecords(records)).unwrap();

    let mut datas = Vec::new();
    for idx in 0..NUM_CLIENTS {
        let mut client_config = ClientConfig::test(true, 600, 2000, 4, false, true, false, false);
        client_config.max_block_wait_delay = Duration::seconds(6);
        let store = create_test_store();
        initialize_genesis_state(store.clone(), &genesis, None);

        let sync_jobs_actions = SyncJobsActions::new(
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<ClientSenderForSyncJobsMessage, _>(),
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<SyncJobsSenderForSyncJobsMessage, _>(),
        );
        let chain_genesis = ChainGenesis::new(&genesis.config);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let shard_tracker = ShardTracker::new(TrackedConfig::AllShards, epoch_manager.clone());
        let state_sync_adapter = Arc::new(RwLock::new(SyncAdapter::new(
            builder.sender().for_index(idx).into_sender(),
            builder.sender().for_index(idx).into_sender(),
        )));
        let runtime_adapter = NightshadeRuntime::test_with_trie_config(
            Path::new("."),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
            TrieConfig { load_mem_tries_for_all_shards: true, ..Default::default() },
            StateSnapshotType::ForReshardingOnly,
        );

        let client = Client::new(
            builder.clock(),
            client_config.clone(),
            chain_genesis,
            epoch_manager.clone(),
            shard_tracker.clone(),
            state_sync_adapter,
            runtime_adapter,
            builder.sender().for_index(idx).into_multi_sender(),
            builder.sender().for_index(idx).into_sender(),
            Some(Arc::new(create_test_signer(accounts[idx].as_str()))),
            true,
            [0; 32],
            None,
            Arc::new(
                builder
                    .sender()
                    .for_index(idx)
                    .into_async_computation_spawner(|_| Duration::milliseconds(80)),
            ),
        )
        .unwrap();

        let shards_manager = ShardsManager::new(
            builder.clock(),
            Some(accounts[idx].clone()),
            epoch_manager,
            shard_tracker,
            builder.sender().for_index(idx).into_sender(),
            builder.sender().for_index(idx).into_sender(),
            ReadOnlyChunksStore::new(store),
            client.chain.head().unwrap(),
            client.chain.header_head().unwrap(),
        );

        let client_actions = ClientActions::new(
            builder.clock(),
            client,
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<ClientSenderForClientMessage, _>(),
            client_config,
            PeerId::random(),
            builder.sender().for_index(idx).into_multi_sender(),
            None,
            noop().into_sender(),
            None,
            Default::default(),
            None,
            builder
                .sender()
                .for_index(idx)
                .into_wrapped_multi_sender::<SyncJobsSenderForClientMessage, _>(),
            Box::new(builder.sender().for_index(idx).into_future_spawner()),
        )
        .unwrap();

        let data = TestData {
            dummy: (),
            account: accounts[idx].clone(),
            client: client_actions,
            sync_jobs: sync_jobs_actions,
            shards_manager,
        };
        datas.push(data);
    }

    let mut test = builder.build(datas);
    for idx in 0..NUM_CLIENTS {
        // Futures, adhoc events, async computations.
        test.register_handler(drive_futures().widen().for_index(idx));
        test.register_handler(handle_adhoc_events::<TestData>().widen().for_index(idx));
        test.register_handler(drive_async_computations().widen().for_index(idx));

        // Delayed actions.
        test.register_handler(
            drive_delayed_action_runners::<ClientActions>().widen().for_index(idx),
        );
        test.register_handler(
            drive_delayed_action_runners::<ShardsManager>().widen().for_index(idx),
        );

        // Messages to the client.
        test.register_handler(
            forward_client_messages_from_network_to_client_actions().widen().for_index(idx),
        );
        test.register_handler(
            forward_client_messages_from_client_to_client_actions().widen().for_index(idx),
        );
        test.register_handler(
            forward_client_messages_from_sync_jobs_to_client_actions().widen().for_index(idx),
        );
        test.register_handler(forward_client_messages_from_shards_manager().widen().for_index(idx));
        // TODO: handle state sync adapter -> client.

        // Messages to the SyncJobs component.
        test.register_handler(
            forward_sync_jobs_messages_from_client_to_sync_jobs_actions(
                test.sender().for_index(idx).into_future_spawner(),
            )
            .widen()
            .for_index(idx),
        );
        // TODO: handle SyncJobs -> SyncJobs.

        // Messages to the ShardsManager component.
        test.register_handler(forward_client_request_to_shards_manager().widen().for_index(idx));
        test.register_handler(forward_network_request_to_shards_manager().widen().for_index(idx));

        // Messages to the network layer; multi-node messages are handled below.
        test.register_handler(ignore_events::<SetChainInfo>().widen().for_index(idx));
    }
    // Handles network routing. Outgoing messages are handled by emitting incoming messages to the
    // appropriate component of the appropriate node index.
    test.register_handler(route_network_messages_to_client(NETWORK_DELAY));
    test.register_handler(route_shards_manager_network_messages(NETWORK_DELAY));

    // Bootstrap the test by starting the components.
    // We use adhoc events for these, just so that the visualizer can see these as events rather
    // than happening outside of the TestLoop framework. Other than that, we could also just remove
    // the send_adhoc_event part and the test would still work.
    for idx in 0..NUM_CLIENTS {
        let sender = test.sender().for_index(idx);
        test.sender().for_index(idx).send_adhoc_event("start_client", move |data| {
            data.client.start(&mut sender.into_delayed_action_runner());
        });

        let sender = test.sender().for_index(idx);
        test.sender().for_index(idx).send_adhoc_event("start_shards_manager", move |data| {
            data.shards_manager.periodically_resend_chunk_requests(
                &mut sender.into_delayed_action_runner(),
                Duration::milliseconds(100),
            );
        })
    }

    // Give it some condition to stop running at. Here we run the test until the first client
    // reaches height 10003, with a timeout of 5sec (failing if it doesn't reach 10003 in time).
    test.run_until(
        |data| data[0].client.client.chain.head().unwrap().height == 10003,
        Duration::seconds(5),
    );
    for idx in 0..NUM_CLIENTS {
        test.sender().for_index(idx).send_adhoc_event("assertions", |data| {
            let chain = &data.client.client.chain;
            let block = chain.get_block_by_height(10002).unwrap();
            assert_eq!(block.header().chunk_mask(), vec![true],);
        })
    }
    test.run_instant();

    let ft_account: AccountId = "ft".parse().unwrap();
    let ft_account_key = create_user_test_signer(&ft_account);
    let ref_block_hash_1 =
        *test.data[0].client.client.chain.get_block_by_height(10002).unwrap().hash();
    let ft_deploy_contract_tx = SignedTransaction::create_contract(
        1,
        registrar_account.clone(),
        ft_account.clone(),
        ft_contract().to_vec(),
        10 * ONE_NEAR,
        ft_account_key.public_key,
        &create_user_test_signer(&registrar_account),
        ref_block_hash_1,
    );
    let ft_init_tx = SignedTransaction::call(
        2,
        accounts[0].clone(),
        ft_account.clone(),
        &create_user_test_signer(&accounts[0]),
        0,
        "new_default_meta".to_owned(),
        r#"{"owner_id": "account0", "total_supply": "1000000"}"#.as_bytes().to_vec(),
        100000000000000,
        ref_block_hash_1,
    );

    let tx_sender = test
        .sender()
        .for_index(0)
        .into_wrapped_multi_sender::<ClientSenderForNetworkMessage, ClientSenderForNetwork>();
    drop(tx_sender.send_async(ProcessTxRequest {
        transaction: ft_deploy_contract_tx.clone(),
        is_forwarded: false,
        check_only: false,
    }));
    drop(tx_sender.send_async(ProcessTxRequest {
        transaction: ft_init_tx.clone(),
        is_forwarded: false,
        check_only: false,
    }));

    test.run_until(
        |data| data[0].client.client.chain.head().unwrap().height == 10007,
        Duration::seconds(6),
    );

    assert_matches!(
        test.data.tx_outcome(ft_deploy_contract_tx.get_hash()).status,
        FinalExecutionStatus::SuccessValue(_)
    );
    assert_matches!(
        test.data.tx_outcome(ft_init_tx.get_hash()).status,
        FinalExecutionStatus::SuccessValue(_)
    );

    let mut storage_deposit_txn_hashes = Vec::new();
    for i in 1..accounts.len() {
        let tx = SignedTransaction::call(
            1,
            accounts[i].clone(),
            ft_account.clone(),
            &create_user_test_signer(&accounts[i]),
            1 * ONE_NEAR,
            "storage_deposit".to_owned(),
            format!(r#"{{"account_id": "{}"}}"#, accounts[i].as_str()).as_bytes().to_vec(),
            100000000000000,
            ref_block_hash_1,
        );
        storage_deposit_txn_hashes.push(tx.get_hash());
        drop(tx_sender.send_async(ProcessTxRequest {
            transaction: tx,
            is_forwarded: false,
            check_only: false,
        }));
    }

    test.run_until(
        |data| data[0].client.client.chain.head().unwrap().height == 10011,
        Duration::seconds(6),
    );

    for txn_hash in storage_deposit_txn_hashes {
        assert_matches!(
            test.data.tx_outcome(txn_hash).status,
            FinalExecutionStatus::SuccessValue(_)
        );
    }

    let mut balances =
        accounts.iter().cloned().map(|account| (account, 0u128)).collect::<HashMap<_, _>>();
    balances.insert(accounts[0].clone(), 1000000);

    let mut transfer_txn_hashes = Vec::new();
    for i in 1..accounts.len() {
        let amount = 1 * (i as u128 + 1);
        let tx = SignedTransaction::call(
            100 + i as u64,
            accounts[0].clone(),
            ft_account.clone(),
            &create_user_test_signer(&accounts[0]),
            1,
            "ft_transfer".to_owned(),
            format!(
                r#"{{"receiver_id": "{}", "amount": "{}", "memo": null}}"#,
                accounts[i].as_str(),
                amount
            )
            .as_bytes()
            .to_vec(),
            100000000000000,
            ref_block_hash_1,
        );
        transfer_txn_hashes.push(tx.get_hash());
        *balances.get_mut(&accounts[0]).unwrap() -= amount;
        *balances.get_mut(&accounts[i]).unwrap() += amount;
        drop(tx_sender.send_async(ProcessTxRequest {
            transaction: tx,
            is_forwarded: false,
            check_only: false,
        }));
    }
    test.run_until(
        |data| data[0].client.client.chain.head().unwrap().height == 10018,
        Duration::seconds(20),
    );

    for (i, txn_hash) in transfer_txn_hashes.into_iter().enumerate() {
        assert_matches!(
            test.data.tx_outcome(txn_hash).status,
            FinalExecutionStatus::SuccessValue(_),
            "Transfer #{} failed",
            i,
        );
    }

    for account in &accounts {
        let balance = test.data.view_call(
            &ft_account,
            "ft_balance_of",
            format!(r#"{{"account_id": "{}"}}"#, account.as_str()).as_bytes(),
        );
        let balance = String::from_utf8(balance).unwrap();
        assert_eq!(
            balance,
            format!("\"{}\"", *balances.get(account).unwrap()),
            "Account balance mismatch for account {}",
            account
        );
    }

    // Give the test a chance to finish off remaining important events in the event loop, which can
    // be important for properly shutting down the nodes.
    test.finish_remaining_events(Duration::seconds(1));
}
