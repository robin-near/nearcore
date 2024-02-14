use super::{delay_sender::DelaySender, event_handler::LoopEventHandler};
use crate::futures::DelayedActionRunner;
use crate::{futures::FutureSpawner, messaging::CanSend};
use futures::future::BoxFuture;
use futures::task::{waker_ref, ArcWake};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::task::Context;

// Support for futures in TestLoop.
//
// There are two key features this file provides for TestLoop:
//
//   1. A general way to spawn futures and have the TestLoop drive the futures.
//      To support this, add () to the Data, add Arc<TestLoopTask> as an Event,
//      and add DriveFutures as a handler. Finally, pass
//      DelaySender<Arc<TestLoopTask>> as the &dyn FutureSpawner to any
//      component that needs to spawn futures.
//
//      This causes any futures spawned during the test to end up as an event
//      (an Arc<TestLoopTask>) in the test loop. The event will eventually be
//      executed by the DriveFutures handler, which will drive the future
//      until it is either suspended or completed. If suspended, then the waker
//      of the future (called when the future is ready to resume) will place
//      the Arc<TestLoopTask> event back into the test loop to be executed
//      again.
//
//   2. A way to send a message to the TestLoop and expect a response as a
//      future, which will resolve whenever the TestLoop handles the message.
//      To support this, use MessageExpectingResponse<Request, Response> as the
//      event type, and in the handler, call (event.responder)(result)
//      (possibly asynchronously) to complete the future.
//
//      This is needed to support the AsyncSender interface, which is required
//      by some components as they expect a response to each message. The way
//      this is implemented is by implementing a conversion from
//      DelaySender<MessageExpectingResponse<Request, Response>> to
//      AsyncSender<Request, Response>.

/// A message, plus a response callback. This should be used as the event type
/// when testing an Actix component that's expected to return a result.
///
/// The response is used to complete the future that is returned by
/// our `AsyncSender::send_async` implementation.

pub struct TestLoopTask {
    future: Mutex<Option<BoxFuture<'static, ()>>>,
    sender: DelaySender<Arc<TestLoopTask>>,
    description: String,
}

impl ArcWake for TestLoopTask {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let clone = arc_self.clone();
        arc_self.sender.send(clone);
    }
}

impl Debug for TestLoopTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Task").field(&self.description).finish()
    }
}

/// Drives any Arc<TestLoopTask> events (futures spawned by our implementation
/// of FutureSpawner) that are remaining in the loop.
pub fn drive_futures() -> LoopEventHandler<(), Arc<TestLoopTask>> {
    LoopEventHandler::new_simple(|task: Arc<TestLoopTask>, _| {
        // The following is copied from the Rust async book.
        // Take the future, and if it has not yet completed (is still Some),
        // poll it in an attempt to complete it.
        let mut future_slot = task.future.lock().unwrap();
        if let Some(mut future) = future_slot.take() {
            let waker = waker_ref(&task);
            let context = &mut Context::from_waker(&*waker);
            if future.as_mut().poll(context).is_pending() {
                // We're still not done processing the future, so put it
                // back in its task to be run again in the future.
                *future_slot = Some(future);
            }
        }
    })
}

/// A DelaySender<Arc<TestLoopTask>> is a FutureSpawner that can be used to
/// spawn futures into the test loop. We give it a convenient alias.
pub type TestLoopFutureSpawner = DelaySender<Arc<TestLoopTask>>;

impl FutureSpawner for TestLoopFutureSpawner {
    fn spawn_boxed(&self, description: &str, f: BoxFuture<'static, ()>) {
        let task = Arc::new(TestLoopTask {
            future: Mutex::new(Some(f)),
            sender: self.clone(),
            description: description.to_string(),
        });
        self.send(task);
    }
}

pub type TestLoopDelayedActionEvent<T> =
    Box<dyn FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + 'static>;

pub fn drive_delayed_action_runners<T>() -> LoopEventHandler<T, TestLoopDelayedActionEvent<T>> {
    LoopEventHandler::new(|event, data, ctx| {
        let mut runner = TestLoopDelayedActionRunner { sender: ctx.sender.clone() };
        event(data, &mut runner);
        Ok(())
    })
}

pub struct TestLoopDelayedActionRunner<T> {
    sender: DelaySender<TestLoopDelayedActionEvent<T>>,
}

impl<T> DelayedActionRunner<T> for TestLoopDelayedActionRunner<T> {
    fn run_later_boxed(
        &mut self,
        dur: std::time::Duration,
        f: Box<dyn FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + 'static>,
    ) {
        self.sender.send_with_delay(f, dur.try_into().unwrap());
    }
}
