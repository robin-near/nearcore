use std::future::{pending, Future};
use std::path::PathBuf;
use std::sync::Arc;

use actix::clock::sleep;
use clap::Parser;
use near_async::messaging::noop_sender;
use near_chain::ChainGenesis;
use near_client::start_view_client;
use near_entity_debug::entity_debug::EntityDebugHandlerImpl;
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_store::{Mode, NodeStorage};
use nearcore::{NearConfig, NightshadeRuntime};

#[derive(Parser)]
pub(crate) struct RunRpcCommand {}

impl RunRpcCommand {
    pub(crate) async fn run(&self, home_dir: &PathBuf, config: NearConfig) -> anyhow::Result<()> {
        let opener = NodeStorage::opener(
            home_dir,
            config.client_config.archive,
            &config.config.store,
            config.config.cold_store.as_ref(),
        );
        let storage = opener.open_in_mode(Mode::ReadOnly)?;

        let epoch_manager =
            EpochManager::new_arc_handle(storage.get_hot_store(), &config.genesis.config);
        let shard_tracker = ShardTracker::new(
            TrackedConfig::from_config(&config.client_config),
            epoch_manager.clone(),
        );
        let runtime = NightshadeRuntime::from_config(
            home_dir,
            storage.get_hot_store(),
            &config,
            epoch_manager.clone(),
        );

        let chain_genesis = ChainGenesis::new(&config.genesis);
        let adv = near_client::adversarial::Controls::new(config.client_config.archive);

        let view_client = start_view_client(
            config.validator_signer.as_ref().map(|signer| signer.validator_id().clone()),
            chain_genesis.clone(),
            epoch_manager.clone(),
            shard_tracker,
            runtime.clone(),
            Arc::new(noop_sender()).into(),
            config.client_config.clone(),
            adv.clone(),
        );
        let hot_store = storage.get_hot_store();

        let rpc_config = config.rpc_config.clone().unwrap();
        let entity_debug_handler =
            EntityDebugHandlerImpl { epoch_manager, runtime, store: hot_store };
        let _rpc_server = near_jsonrpc::start_http(
            rpc_config,
            config.genesis.config.clone(),
            Arc::new(noop_sender()).into(),
            view_client.clone(),
            None,
            Arc::new(entity_debug_handler),
        );
        pending::<()>().await;
        Ok(())
    }
}
