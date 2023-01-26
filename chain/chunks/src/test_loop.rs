use near_async::test_loop::{LoopEventHandler, TryIntoOrSelf};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;

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
