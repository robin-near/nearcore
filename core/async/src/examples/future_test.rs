use std::{sync::Arc, time::Duration};

use derive_enum_from_into::{EnumFrom, EnumTryInto};
use futures::{future::BoxFuture, FutureExt};

use crate::{
    futures::FutureSpawner,
    messaging::{AsyncSender, CanSend, CanSendAsync, IntoAsyncSender},
    test_loop::{
        event_handler::{capture_events, LoopEventHandler, LoopEventHandlerHelpers},
        futures::{DriveFutures, MessageExpectingResponse, Task},
        TestLoopBuilder,
    },
};

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct TestData {
    dummy: (),
    output: Vec<Response>,
    component: Component,
}

#[derive(Debug)]
struct Request(String);

#[derive(Debug)]
struct Response(String);

#[derive(Debug)]
struct InnerRequest(String);

#[derive(Debug)]
struct InnerResponse(String);

struct Component {
    inner_sender: AsyncSender<InnerRequest, InnerResponse>,
}

impl Component {
    fn new(inner_sender: AsyncSender<InnerRequest, InnerResponse>) -> Self {
        Self { inner_sender }
    }

    fn handle(&mut self, request: Request) -> BoxFuture<'static, Response> {
        let inner_request = InnerRequest(request.0.clone());
        let sender = self.inner_sender.clone();
        async move {
            let inner_response = sender.send_async(inner_request).await;
            Response(inner_response.0.repeat(2))
        }
        .boxed()
    }
}

#[derive(Debug, EnumTryInto, EnumFrom)]
enum TestEvent {
    Response(Response),
    Request(MessageExpectingResponse<Request, Response>),
    InnerRequest(MessageExpectingResponse<InnerRequest, InnerResponse>),
    Task(Arc<Task>),
}

struct RequestHandler;

impl LoopEventHandler<Component, MessageExpectingResponse<Request, Response>> for RequestHandler {
    fn handle(
        &mut self,
        event: MessageExpectingResponse<Request, Response>,
        data: &mut Component,
    ) -> Option<MessageExpectingResponse<Request, Response>> {
        (event.responder)(data.handle(event.message).boxed());
        None
    }
}

struct InnerRequestHandler;

impl LoopEventHandler<(), MessageExpectingResponse<InnerRequest, InnerResponse>>
    for InnerRequestHandler
{
    fn handle(
        &mut self,
        event: MessageExpectingResponse<InnerRequest, InnerResponse>,
        _data: &mut (),
    ) -> Option<MessageExpectingResponse<InnerRequest, InnerResponse>> {
        (event.responder)(async move { InnerResponse(event.message.0 + "!") }.boxed());
        None
    }
}

#[test]
fn test_futures() {
    let builder = TestLoopBuilder::<TestEvent>::new();
    let sender = builder.sender();
    let mut test = builder.build(TestData {
        dummy: (),
        output: vec![],
        component: Component::new(sender.clone().into_async_sender()),
    });
    test.register_handler(DriveFutures.widen());
    test.register_handler(capture_events::<Response>().widen());
    test.register_handler(RequestHandler.widen());
    test.register_handler(InnerRequestHandler.widen());

    let sender1 = sender.clone();
    let future1 = async move {
        let response = sender1.send_async(Request("hello".to_string())).await;
        sender1.send(response);
    };
    sender.spawn_boxed("test future", future1.boxed());
    test.run(Duration::from_secs(1));
}
