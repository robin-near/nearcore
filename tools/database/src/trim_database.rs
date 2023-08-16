use std::collections::HashMap;
use std::path::Path;

use near_primitives::block::Tip;
use near_primitives::block_header::BlockHeader;
use near_primitives::borsh::BorshSerialize;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use near_store::{DBCol, NodeStorage, HEADER_HEAD_KEY, TAIL_KEY};

#[derive(clap::Args)]
pub(crate) struct TrimDatabaseCommand {}

impl TrimDatabaseCommand {
    pub(crate) fn run(&self, home_dir: &Path) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(
            &home_dir,
            near_chain_configs::GenesisValidationMode::UnsafeFast,
        )?;
        let opener = NodeStorage::opener(
            home_dir,
            near_config.config.archive,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
        );
        let storage = opener.open()?;
        let store = storage.get_hot_store();

        let tail = store.get_ser::<BlockHeight>(DBCol::BlockMisc, TAIL_KEY)?.expect("No tail");
        let head =
            store.get_ser::<Tip>(DBCol::BlockMisc, HEADER_HEAD_KEY)?.expect("No header head");

        let mut block_headers_to_retain = HashMap::<CryptoHash, BlockHeader>::new();
        for block_height in tail..=head.height {
            let block_hashes = store.get_ser::<Vec<CryptoHash>>(
                DBCol::HeaderHashesByHeight,
                &block_height.try_to_vec().unwrap(),
            )?;
            if let Some(hashes) = block_hashes {
                for hash in hashes {
                    let header = store
                        .get_ser::<BlockHeader>(DBCol::BlockHeader, &hash.try_to_vec().unwrap())?
                        .expect(&format!("Header missing: {}", hash));
                    block_headers_to_retain.insert(hash, header);
                }
            }
        }
        println!(
            "Found {} block headers to retain from {} to {}",
            block_headers_to_retain.len(),
            tail,
            head.height
        );

        println!("Dropping the entire BlockHeader column...");
        let mut update = store.store_update();
        update.delete_all(DBCol::BlockHeader);
        update.commit()?;

        println!("Inserting the retained block headers back into the DB...");
        let mut update = store.store_update();
        for (hash, header) in block_headers_to_retain {
            update.insert_ser(DBCol::BlockHeader, &hash.try_to_vec().unwrap(), &header)?;
        }
        update.commit()?;

        println!("Dropping the CachedContractCode column...");
        let mut update = store.store_update();
        update.delete_all(DBCol::CachedContractCode);
        update.commit()?;

        println!("All done.");
        Ok(())
    }
}
