use std::{
    fmt::Debug,
    future::Future,
    sync::{Arc, Mutex},
    task::{Context, Waker},
    time::Duration,
};

use futures::{
    future::BoxFuture,
    task::{waker_ref, ArcWake},
    FutureExt,
};

use crate::{
    futures::FutureSpawner,
    messaging::{self, CanSend},
};

use super::{delay_sender::DelaySender, event_handler::LoopEventHandler};

pub(crate) struct TestLoopFutureState<T> {
    result: Option<BoxFuture<'static, T>>,
    waker: Option<Waker>,
}

pub(crate) struct TestLoopFuture<T> {
    state: Arc<Mutex<TestLoopFutureState<T>>>,
}

impl<T> Future for TestLoopFuture<T> {
    type Output = BoxFuture<'static, T>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut state = self.state.lock().unwrap();
        if let Some(result) = state.result.take() {
            std::task::Poll::Ready(result)
        } else {
            state.waker = Some(cx.waker().clone());
            std::task::Poll::Pending
        }
    }
}

impl<T> TestLoopFuture<T> {
    pub(crate) fn new() -> Self {
        Self { state: Arc::new(Mutex::new(TestLoopFutureState { result: None, waker: None })) }
    }

    pub(crate) fn state(&self) -> Arc<Mutex<TestLoopFutureState<T>>> {
        self.state.clone()
    }
}

pub struct MessageExpectingResponse<T, R> {
    pub message: T,
    pub responder: Box<dyn FnOnce(BoxFuture<'static, R>) + Send>,
}

impl<T: Debug, R> Debug for MessageExpectingResponse<T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MessageWithResponder").field(&self.message).finish()
    }
}

pub(crate) fn create_future_for_message_response<T, R: Send + 'static>(
    message: T,
) -> (impl Future<Output = R>, MessageExpectingResponse<T, R>) {
    let future = TestLoopFuture::new();
    let state = future.state();
    let responder = Box::new(move |response_future| {
        let mut state = state.lock().unwrap();
        state.result = Some(response_future);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    });
    (future.flatten(), MessageExpectingResponse { message, responder })
}

impl<
        Message: 'static,
        Response: Send + 'static,
        Event: From<MessageExpectingResponse<Message, Response>> + 'static,
    > messaging::CanSendAsync<Message, Response> for DelaySender<Event>
{
    fn send_async(&self, message: Message) -> BoxFuture<'static, Response> {
        let (future, responder) = create_future_for_message_response(message);
        self.send_with_delay(responder.into(), Duration::ZERO);
        future.boxed()
    }
}

pub(crate) struct Task {
    future: Mutex<Option<BoxFuture<'static, ()>>>,
    sender: DelaySender<Arc<Task>>,
    description: String,
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let clone = arc_self.clone();
        arc_self.sender.send(clone);
    }
}

impl Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Task").field(&self.description).finish()
    }
}

pub struct DriveFutures;

impl LoopEventHandler<(), Arc<Task>> for DriveFutures {
    fn handle(&mut self, task: Arc<Task>, _: &mut ()) -> Option<Arc<Task>> {
        // The following is copied from the Rust async book.
        // Take the future, and if it has not yet completed (is still Some),
        // poll it in an attempt to complete it.
        let mut future_slot = task.future.lock().unwrap();
        if let Some(mut future) = future_slot.take() {
            // Create a `LocalWaker` from the task itself
            let waker = waker_ref(&task);
            let context = &mut Context::from_waker(&*waker);
            // `BoxFuture<T>` is a type alias for
            // `Pin<Box<dyn Future<Output = T> + Send + 'static>>`.
            // We can get a `Pin<&mut dyn Future + Send + 'static>`
            // from it by calling the `Pin::as_mut` method.
            if future.as_mut().poll(context).is_pending() {
                // We're not done processing the future, so put it
                // back in its task to be run again in the future.
                *future_slot = Some(future);
            }
        }
        None
    }
}

impl<Event: From<Arc<Task>> + 'static> FutureSpawner for DelaySender<Event> {
    fn spawn_boxed(&self, description: &str, f: BoxFuture<'static, ()>) {
        let task = Arc::new(Task {
            future: Mutex::new(Some(f)),
            sender: self.clone().narrow(),
            description: description.to_string(),
        });
        self.send(task);
    }
}
