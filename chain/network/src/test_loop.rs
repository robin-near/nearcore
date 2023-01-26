use near_primitives::{hash::CryptoHash, types::AccountId};

use crate::types::{NetworkRequests, PeerMessage};

pub type OutgoingNetworkMessage = NetworkRequests;
pub type IncomingNetworkMessage = PeerMessage;

pub trait SupportsRoutingLookup {
    fn index_for_account(&self, account: &AccountId) -> usize;
    fn index_for_hash(&self, hash: CryptoHash) -> usize;
}

impl<InnerData: AsRef<AccountId>> SupportsRoutingLookup for Vec<InnerData> {
    fn index_for_account(&self, account: &AccountId) -> usize {
        self.iter().position(|data| data.as_ref() == account).expect("Account not found")
    }

    fn index_for_hash(&self, hash: CryptoHash) -> usize {
        self.iter()
            .position(|data| CryptoHash::hash_bytes(data.as_ref().as_bytes()) == hash)
            .expect("Hash not found")
    }
}
