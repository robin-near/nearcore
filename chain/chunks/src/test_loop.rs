use std::time::{Duration, Instant};

use near_async::test_loop::{
    delay_sender::DelaySender,
    event_handler::{LoopEventHandler, TryIntoOrSelf},
};
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork,
    test_loop::SupportsRoutingLookup,
    types::{NetworkRequests, PeerManagerMessageRequest},
};
use near_primitives::{hash::CryptoHash, types::AccountId};

use crate::{adapter::ShardsManagerRequestFromClient, ShardsManager};

pub struct ForwardClientRequestToShardsManager;
pub struct ForwardNetworkRequestToShardsManager;

impl LoopEventHandler<ShardsManager, ShardsManagerRequestFromClient>
    for ForwardClientRequestToShardsManager
{
    fn handle(
        &mut self,
        event: ShardsManagerRequestFromClient,
        data: &mut ShardsManager,
    ) -> Option<ShardsManagerRequestFromClient> {
        data.handle_client_request(event);
        None
    }
}

impl LoopEventHandler<ShardsManager, ShardsManagerRequestFromNetwork>
    for ForwardNetworkRequestToShardsManager
{
    fn handle(
        &mut self,
        event: ShardsManagerRequestFromNetwork,
        data: &mut ShardsManager,
    ) -> Option<ShardsManagerRequestFromNetwork> {
        data.handle_network_request(event);
        None
    }
}

pub struct RouteShardsManagerNetworkMessages<Event: From<ShardsManagerRequestFromNetwork>> {
    incoming_message_sender: Option<DelaySender<(usize, Event)>>,
    network_delay: Duration,
}

impl<Event: From<ShardsManagerRequestFromNetwork>> RouteShardsManagerNetworkMessages<Event> {
    pub fn new(network_delay: Duration) -> Self {
        Self { incoming_message_sender: None, network_delay }
    }

    fn send_incoming<Message: Into<Event>>(&self, target_idx: usize, msg: Message) {
        self.incoming_message_sender
            .as_ref()
            .unwrap()
            .send_with_delay((target_idx, msg.into()), self.network_delay);
    }
}

impl<
        Data: AsRef<AccountId>,
        Event: TryIntoOrSelf<PeerManagerMessageRequest>
            + From<PeerManagerMessageRequest>
            + From<ShardsManagerRequestFromNetwork>,
    > LoopEventHandler<Vec<Data>, (usize, Event)> for RouteShardsManagerNetworkMessages<Event>
{
    fn init(&mut self, sender: DelaySender<(usize, Event)>) {
        self.incoming_message_sender = Some(sender);
    }
    fn handle(&mut self, event: (usize, Event), data: &mut Vec<Data>) -> Option<(usize, Event)> {
        let (idx, event) = event;
        match event.try_into_or_self() {
            Ok(message) => {
                match message {
                    PeerManagerMessageRequest::NetworkRequests(request) => match request {
                        NetworkRequests::PartialEncodedChunkRequest { target, request, .. } => {
                            let target_idx = data.index_for_account(&target.account_id.unwrap());
                            self.send_incoming(
                                target_idx,
                                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                                    partial_encoded_chunk_request: request,
                                    route_back: CryptoHash::hash_bytes(data[idx].as_ref().as_bytes()),
                                },
                            );
                            None
                        }
                        NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
                            let target_idx = data.index_for_hash(route_back);
                            self.send_incoming(
                                target_idx,
                                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                                    partial_encoded_chunk_response: response,
                                    received_time: Instant::now(), // TODO: use clock
                                }
                            );
                            None
                        }
                        NetworkRequests::PartialEncodedChunkMessage {
                            account_id,
                            partial_encoded_chunk,
                        } => {
                            let target_idx = data.index_for_account(&account_id);
                            self.send_incoming(
                                target_idx,
                                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                                    partial_encoded_chunk.into(),
                                ),
                            );
                            None
                        }
                        NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                            let target_idx = data.index_for_account(&account_id);
                            self.send_incoming(
                                target_idx,
                                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(
                                    forward,
                                ),
                            );
                            None
                        }
                        other_message => Some((
                            idx,
                            PeerManagerMessageRequest::NetworkRequests(other_message).into(),
                        )),
                    },
                    message => Some((idx, message.into())),
                }
            }
            Err(event) => Some((idx, event)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShardsManagerResendRequests;
