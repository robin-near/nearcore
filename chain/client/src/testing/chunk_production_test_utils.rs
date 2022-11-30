use near_client_primitives::types::Error;
use near_primitives::{merkle::MerklePath, receipt::Receipt, sharding::EncodedShardChunk};

use crate::Client;

pub trait ClientTestExt {
    fn persist_and_distribute_encoded_chunk(
        &mut self,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        receipts: Vec<Receipt>,
    ) -> Result<(), Error>;
}

impl ClientTestExt for Client {
    fn persist_and_distribute_encoded_chunk(
        &mut self,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        receipts: Vec<Receipt>,
    ) -> Result<(), Error> {
        self.production.as_mut().unwrap().persist_and_distribute_encoded_chunk(
            self.chain.mut_store(),
            &mut self.shards_mgr,
            encoded_chunk,
            merkle_paths,
            receipts,
        )
    }
}
