use std::time::Instant;

use near_async::{
    messaging::ArcSender,
    test_loop::{LoopEventHandler, TryIntoOrSelf},
};
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork,
    test_loop::SupportsRoutingLookup,
    types::{NetworkRequests, PeerManagerMessageRequest},
};
use near_primitives::{hash::CryptoHash, types::AccountId};

use crate::{adapter::ShardsManagerRequestFromClient, ShardsManager};

pub struct ForwardClientRequestToShardsManager {}
pub struct ForwardNetworkRequestToShardsManager {}

impl<Data: AsMut<ShardsManager>, Event: TryIntoOrSelf<ShardsManagerRequestFromClient>>
    LoopEventHandler<Data, Event> for ForwardClientRequestToShardsManager
{
    fn handle(&mut self, event: Event, data: &mut Data) -> Option<Event> {
        match event.try_into_or_self() {
            Ok(request) => {
                data.as_mut().handle_client_request(request);
                None
            }
            Err(event) => Some(event),
        }
    }
}

impl<Data: AsMut<ShardsManager>, Event: TryIntoOrSelf<ShardsManagerRequestFromNetwork>>
    LoopEventHandler<Data, Event> for ForwardNetworkRequestToShardsManager
{
    fn handle(&mut self, event: Event, data: &mut Data) -> Option<Event> {
        match event.try_into_or_self() {
            Ok(request) => {
                data.as_mut().handle_network_request(request);
                None
            }
            Err(event) => Some(event),
        }
    }
}

pub struct RouteShardsManagerNetworkMessages {
    incoming_message_sender: ArcSender<(usize, ShardsManagerRequestFromNetwork)>,
}

impl<
        Data: AsRef<AccountId>,
        Event: TryIntoOrSelf<PeerManagerMessageRequest>
            + From<PeerManagerMessageRequest>
            + From<ShardsManagerRequestFromNetwork>,
    > LoopEventHandler<Vec<Data>, (usize, Event)> for RouteShardsManagerNetworkMessages
{
    fn handle(&mut self, event: (usize, Event), data: &mut Vec<Data>) -> Option<(usize, Event)> {
        let (idx, event) = event;
        match event.try_into_or_self() {
            Ok(message) => match message {
                PeerManagerMessageRequest::NetworkRequests(request) => match request {
                    NetworkRequests::PartialEncodedChunkRequest { target, request, .. } => {
                        let target_idx = data.index_for_account(&target.account_id.unwrap());
                        self.incoming_message_sender.send((
                            target_idx,
                            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                                partial_encoded_chunk_request: request,
                                route_back: CryptoHash::hash_bytes(data[idx].as_ref().as_bytes()),
                            },
                        ));
                        None
                    }
                    NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
                        let target_idx = data.index_for_hash(route_back);
                        self.incoming_message_sender.send((
                            target_idx,
                            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                                partial_encoded_chunk_response: response,
                                received_time: Instant::now(), // TODO: use clock
                            },
                        ));
                        None
                    }
                    NetworkRequests::PartialEncodedChunkMessage {
                        account_id,
                        partial_encoded_chunk,
                    } => {
                        let target_idx = data.index_for_account(&account_id);
                        self.incoming_message_sender.send((
                            target_idx,
                            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                                partial_encoded_chunk.into(),
                            ),
                        ));
                        None
                    }
                    NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                        let target_idx = data.index_for_account(&account_id);
                        self.incoming_message_sender.send((
                            target_idx,
                            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(
                                forward,
                            ),
                        ));
                        None
                    }
                    other_message => Some((
                        idx,
                        PeerManagerMessageRequest::NetworkRequests(other_message).into(),
                    )),
                },
                message => Some((idx, message.into())),
            },
            Err(event) => Some((idx, event)),
        }
    }
}
