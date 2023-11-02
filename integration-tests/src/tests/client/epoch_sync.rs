use crate::nearcore_utils::{add_blocks, setup_configs_with_epoch_length};
use crate::test_helpers::heavy_test;
use actix::Actor;
use actix_rt::System;
use futures::{future, FutureExt};
use near_actix_test_utils::run_actix;
use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_client_primitives::types::GetBlock;
use near_crypto::{InMemorySigner, KeyType};
use near_network::test_utils::WaitOrTimeoutActor;
use near_o11y::testonly::{init_integration_logger, init_test_logger};
use near_o11y::WithSpanContextExt;
use near_primitives::epoch_manager::epoch_sync::EpochSyncInfo;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::BlockHeight;
use near_store::Mode::ReadOnly;
use near_store::{DBCol, NodeStorage};
use nearcore::config::GenesisExt;
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use nearcore::{start_with_config, NearConfig};
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};

fn generate_transactions(last_hash: &CryptoHash, h: BlockHeight) -> Vec<SignedTransaction> {
    let mut txs = vec![];
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    if h == 1 {
        txs.push(SignedTransaction::from_actions(
            h,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::DeployContract(DeployContractAction {
                code: near_test_contracts::rs_contract().to_vec(),
            })],
            *last_hash,
        ));
    }

    for i in 0..5 {
        txs.push(SignedTransaction::from_actions(
            h * 10 + i,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "write_random_value".to_string(),
                args: vec![],
                gas: 100_000_000_000_000,
                deposit: 0,
            }))],
            *last_hash,
        ));
    }

    for i in 0..5 {
        txs.push(SignedTransaction::send_money(
            h * 10 + i,
            "test0".parse().unwrap(),
            "test1".parse().unwrap(),
            &signer,
            1,
            *last_hash,
        ));
    }
    txs
}

/// Produce 4 epochs with some transactions.
/// At the end of each epoch check that `EpochSyncInfo` has been recorded.
#[test]
fn test_continuous_epoch_sync_info_population() {
    init_test_logger();

    let epoch_length = 5;
    let max_height = epoch_length * 4 + 1;

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);

    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let mut last_hash = *env.clients[0].chain.genesis().hash();

    for h in 1..max_height {
        for tx in generate_transactions(&last_hash, h) {
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        last_hash = *block.hash();

        if env.clients[0].epoch_manager.is_next_block_epoch_start(&last_hash).unwrap() {
            let epoch_id = block.header().epoch_id().clone();

            tracing::debug!("Checking epoch: {:?}", &epoch_id);
            assert!(env.clients[0].chain.store().get_epoch_sync_info(&epoch_id).is_ok());
            tracing::debug!("OK");
        }
    }
}

/// Produce 4 epochs + 10 blocks.
/// Start second node without epoch sync, but with state sync.
/// Sync second node to first node (at least headers).
/// Check that it has all EpochSyncInfo records and all of them are correct.
///
/// The header sync part is based on `integration-tests::nearcore::sync_nodes::sync_nodes`.
#[test]
fn test_continuous_epoch_sync_info_population_on_header_sync() {
    heavy_test(|| {
        init_integration_logger();

        let (genesis, genesis_block, mut near1_base, mut near2_base) =
            setup_configs_with_epoch_length(50);

        let dir1_base =
            tempfile::Builder::new().prefix("epoch_sync_info_in_header_sync_1").tempdir().unwrap();
        let dir2_base =
            tempfile::Builder::new().prefix("epoch_sync_info_in_header_sync_2").tempdir().unwrap();
        let epoch_ids_base = Arc::new(RwLock::new(HashSet::new()));

        let near1 = near1_base.clone();
        let near2 = near2_base.clone();
        let dir1_path = dir1_base.path();
        let dir2_path = dir2_base.path();
        let epoch_ids = epoch_ids_base.clone();

        run_actix(async move {
            // Start first node
            let nearcore::NearNode { client: client1, .. } =
                start_with_config(dir1_path, near1).expect("start_with_config");

            // Generate 4 epochs + 10 blocks
            let signer = create_test_signer("other");
            let blocks =
                add_blocks(vec![genesis_block], client1, 210, genesis.config.epoch_length, &signer);

            // Save all finished epoch_ids
            let mut epoch_ids = epoch_ids.write().unwrap();
            for block in blocks[0..200].iter() {
                epoch_ids.insert(block.header().epoch_id().clone());
            }

            // Start second node
            let nearcore::NearNode { view_client: view_client2, .. } =
                start_with_config(dir2_path, near2).expect("start_with_config");

            // Wait for second node's headers to sync.
            // Timeout here means that header sync is not working.
            // Header sync is better debugged through other tests.
            // For example, run `integration-tests::nearcore::sync_nodes::sync_nodes` test,
            // on which this test's setup is based.
            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    let actor = view_client2.send(GetBlock::latest().with_span_context());
                    let actor = actor.then(|res| {
                        match &res {
                            Ok(Ok(b)) if b.header.height == 210 => System::current().stop(),
                            Err(_) => return future::ready(()),
                            _ => {}
                        };
                        future::ready(())
                    });
                    actix::spawn(actor);
                }),
                100,
                120000,
            )
            .start();
        });

        // Open storages of both nodes
        let open_read_only_storage = |home_dir: &Path, near_config: &NearConfig| -> NodeStorage {
            let opener = NodeStorage::opener(home_dir, false, &near_config.config.store, None);
            opener.open_in_mode(ReadOnly).unwrap()
        };

        let store1 = open_read_only_storage(dir1_base.path(), &mut near1_base).get_hot_store();
        let store2 = open_read_only_storage(dir2_base.path(), &mut near2_base).get_hot_store();

        // Check that for every epoch second store has EpochSyncInfo.
        // And that values in both stores are the same.
        let epoch_ids = epoch_ids_base.read().unwrap();
        for epoch_id in epoch_ids.iter() {
            // Check that we have a value for EpochSyncInfo in the synced node
            assert!(
                store2
                    .get_ser::<EpochSyncInfo>(DBCol::EpochSyncInfo, epoch_id.as_ref())
                    .unwrap()
                    .is_some(),
                "{:?}",
                epoch_id
            );
            // Check that it matches value in full node exactly
            assert_eq!(
                store1
                    .get_ser::<EpochSyncInfo>(DBCol::EpochSyncInfo, epoch_id.as_ref())
                    .unwrap()
                    .unwrap(),
                store2
                    .get_ser::<EpochSyncInfo>(DBCol::EpochSyncInfo, epoch_id.as_ref())
                    .unwrap()
                    .unwrap(),
                "{:?}",
                epoch_id
            );
        }
    });
}
