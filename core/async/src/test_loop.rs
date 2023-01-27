use std::{
    collections::BinaryHeap,
    fmt::Debug,
    sync::{self, Arc},
    time::Duration,
};

use near_o11y::{testonly::init_test_logger, tracing::log::info};
use serde::Serialize;

use crate::messaging;

pub struct TestLoop<Data, Event: Debug + Send + 'static> {
    pub data: Data,

    sender: DelaySender<Event>,

    events: BinaryHeap<EventInHeap<Event>>,
    pending_events: sync::mpsc::Receiver<EventInFlight<Event>>,
    next_event_index: usize,
    current_time: Duration,

    handlers_initialized: bool,
    handlers: Vec<Box<dyn LoopEventHandler<Data, Event>>>,
}

struct EventInHeap<Event> {
    event: Event,
    due: Duration,
    id: usize,
}

impl<Event> PartialEq for EventInHeap<Event> {
    fn eq(&self, other: &Self) -> bool {
        self.due == other.due && self.id == other.id
    }
}

impl<Event> Eq for EventInHeap<Event> {}

impl<Event> PartialOrd for EventInHeap<Event> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<Event> Ord for EventInHeap<Event> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.due, self.id).cmp(&(other.due, other.id)).reverse()
    }
}

struct EventInFlight<Event> {
    event: Event,
    delay: Duration,
}

pub struct TestLoopBuilder<Event: Debug + Send + 'static> {
    pending_events: sync::mpsc::Receiver<EventInFlight<Event>>,
    pending_events_sender: DelaySender<Event>,
}

impl<Event: Debug + Send + 'static> TestLoopBuilder<Event> {
    pub fn new() -> Self {
        init_test_logger();
        let (pending_events_sender, pending_events) = sync::mpsc::sync_channel(65536);
        Self {
            pending_events,
            pending_events_sender: DelaySender {
                im: Arc::new(LoopSender { event_sender: pending_events_sender }),
            },
        }
    }

    pub fn sender(&self) -> DelaySender<Event> {
        self.pending_events_sender.clone()
    }

    pub fn build<Data>(self, data: Data) -> TestLoop<Data, Event> {
        TestLoop::new(self.pending_events, self.pending_events_sender, data)
    }
}

pub trait LoopEventHandler<Data, Event> {
    fn init(&mut self, _sender: DelaySender<Event>) {}
    fn handle(&mut self, event: Event, data: &mut Data) -> Option<Event>;
    fn for_index(self, index: usize) -> LoopEventHandlerForIndex<Data, Event>
    where
        Self: Sized + 'static,
    {
        LoopEventHandlerForIndex { handler: Box::new(self), index }
    }
}

#[derive(Serialize)]
struct EventStartLogOutput {
    current_index: usize,
    total_events: usize,
    current_event: String,
    current_time_ms: u64,
}

impl<Data, Event: Debug + Send + 'static> TestLoop<Data, Event> {
    fn new(
        pending_events: sync::mpsc::Receiver<EventInFlight<Event>>,
        sender: DelaySender<Event>,
        data: Data,
    ) -> Self {
        Self {
            data,
            sender,
            events: BinaryHeap::new(),
            pending_events,
            next_event_index: 0,
            current_time: Duration::ZERO,
            handlers_initialized: false,
            handlers: Vec::new(),
        }
    }

    pub fn register_handler<T: LoopEventHandler<Data, Event> + 'static>(&mut self, handler: T) {
        assert!(!self.handlers_initialized, "Cannot register more handlers after run() is called");
        self.handlers.push(Box::new(handler));
    }

    fn maybe_initialize_handlers(&mut self) {
        if self.handlers_initialized {
            return;
        }
        for handler in &mut self.handlers {
            handler.init(self.sender.clone());
        }
    }

    pub fn run(&mut self, duration: Duration) {
        self.maybe_initialize_handlers();
        let deadline = self.current_time + duration;
        'outer: loop {
            while let Ok(event) = self.pending_events.try_recv() {
                self.events.push(EventInHeap {
                    due: self.current_time + event.delay,
                    event: event.event,
                    id: self.next_event_index,
                });
                self.next_event_index += 1;
            }
            match self.events.peek() {
                Some(event) => {
                    if event.due >= deadline {
                        break;
                    }
                }
                None => break,
            }
            let event = self.events.pop().unwrap();
            let json_printout = serde_json::to_string(&EventStartLogOutput {
                current_index: event.id,
                total_events: self.next_event_index,
                current_event: format!("{:?}", event.event),
                current_time_ms: event.due.as_millis() as u64,
            })
            .unwrap();
            info!(target: "test_loop", "TEST_LOOP_EVENT_START {}", json_printout);
            self.current_time = event.due;

            let mut current_event = event.event;
            for handler in &mut self.handlers {
                if let Some(event) = handler.handle(current_event, &mut self.data) {
                    current_event = event;
                } else {
                    continue 'outer;
                }
            }
            panic!("Unhandled event: {:?}", current_event);
        }
    }
}

pub trait TryIntoOrSelf<R>: Sized {
    fn try_into_or_self(self) -> Result<R, Self>;
}

impl<R, T: TryInto<R, Error = derive_more::TryIntoError<T>>> TryIntoOrSelf<R> for T {
    fn try_into_or_self(self) -> Result<R, Self> {
        self.try_into().map_err(|err| err.input)
    }
}

pub struct LoopEventHandlerForIndex<InnerData, InnerEvent> {
    handler: Box<dyn LoopEventHandler<InnerData, InnerEvent>>,
    index: usize,
}

impl<InnerData, InnerEvent: 'static> LoopEventHandler<Vec<InnerData>, (usize, InnerEvent)>
    for LoopEventHandlerForIndex<InnerData, InnerEvent>
{
    fn init(&mut self, sender: DelaySender<(usize, InnerEvent)>) {
        self.handler.init(sender.for_index(self.index));
    }

    fn handle(
        &mut self,
        event: (usize, InnerEvent),
        data: &mut Vec<InnerData>,
    ) -> Option<(usize, InnerEvent)> {
        if event.0 != self.index {
            return Some(event);
        }
        match self.handler.handle(event.1, &mut data[self.index]) {
            Some(event) => Some((self.index, event)),
            None => None,
        }
    }
}

pub struct CaptureEvents<CapturedEvent> {
    _marker: std::marker::PhantomData<CapturedEvent>,
}

impl<CapturedEvent> CaptureEvents<CapturedEvent> {
    pub fn new() -> Self {
        Self { _marker: std::marker::PhantomData }
    }
}

impl<CapturedEvent, Data: AsMut<Vec<CapturedEvent>>, Event: TryIntoOrSelf<CapturedEvent>>
    LoopEventHandler<Data, Event> for CaptureEvents<CapturedEvent>
{
    fn handle(&mut self, event: Event, data: &mut Data) -> Option<Event> {
        match event.try_into_or_self() {
            Ok(event) => {
                data.as_mut().push(event);
                None
            }
            Err(event) => Some(event),
        }
    }
}

trait DelaySenderImpl<Event>: Send + Sync {
    fn send_with_delay(&self, event: Event, delay: Duration);
}

pub struct DelaySender<Event> {
    im: Arc<dyn DelaySenderImpl<Event>>,
}

impl<Event> Clone for DelaySender<Event> {
    fn clone(&self) -> Self {
        Self { im: self.im.clone() }
    }
}

impl<Event> DelaySender<Event> {
    pub fn send_with_delay(&self, event: Event, delay: Duration) {
        self.im.send_with_delay(event, delay);
    }
}

impl<Event: 'static> DelaySender<(usize, Event)> {
    pub fn for_index(self, index: usize) -> DelaySender<Event> {
        DelaySender { im: Arc::new(DelaySenderForIndex { inner: self.im, index }) }
    }
}

pub struct LoopSender<Event: Send + 'static> {
    event_sender: sync::mpsc::SyncSender<EventInFlight<Event>>,
}

impl<Event: Send + 'static> DelaySenderImpl<Event> for LoopSender<Event> {
    fn send_with_delay(&self, event: Event, delay: Duration) {
        self.event_sender.send(EventInFlight { event, delay }).unwrap();
    }
}

impl<Message, Event: From<Message> + 'static> messaging::Sender<Message> for DelaySender<Event> {
    fn send(&self, message: Message) {
        self.send_with_delay(message.into(), Duration::ZERO);
    }
}

#[derive(Clone)]
pub struct DelaySenderForIndex<Event> {
    inner: Arc<dyn DelaySenderImpl<(usize, Event)>>,
    index: usize,
}

impl<Event> DelaySenderImpl<Event> for DelaySenderForIndex<Event> {
    fn send_with_delay(&self, event: Event, delay: Duration) {
        self.inner.send_with_delay((self.index, event), delay);
    }
}
