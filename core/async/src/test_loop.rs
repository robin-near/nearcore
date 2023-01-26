use std::{
    collections::VecDeque,
    fmt::Debug,
    sync::{self, Arc},
};

use crate::messaging;

pub struct TestLoop<Data, Event: Debug + Send + 'static> {
    pub data: Data,
    events: VecDeque<Event>,
    handlers: Vec<Box<dyn LoopEventHandler<Data, Event>>>,
    pending_events: sync::mpsc::Receiver<Event>,
}

pub struct TestLoopBuilder<Event: Debug + Send + 'static> {
    pending_events: sync::mpsc::Receiver<Event>,
    pending_events_sender: sync::mpsc::SyncSender<Event>,
}

impl<Event: Debug + Send + 'static> TestLoopBuilder<Event> {
    pub fn new() -> Self {
        let (pending_events_sender, pending_events) = sync::mpsc::sync_channel(65536);
        Self { pending_events, pending_events_sender }
    }

    pub fn sender(&self) -> LoopSender<Event> {
        LoopSender { event_sender: self.pending_events_sender.clone() }
    }

    pub fn sender_for_index(&self, index: usize) -> LoopSenderForIndex<Event> {
        LoopSenderForIndex { inner_sender: self.sender(), index }
    }

    pub fn build<Data>(self, data: Data) -> TestLoop<Data, Event> {
        TestLoop::new(self.pending_events, data)
    }
}

pub trait LoopEventHandler<Data, Event> {
    fn handle(&mut self, event: Event, data: &mut Data) -> Option<Event>;

    fn indexed(self) -> IndexedLoopEventHandler<Data, Event>
    where
        Self: Sized + 'static,
    {
        IndexedLoopEventHandler { handler: Box::new(self) }
    }
}

impl<Data, Event: Debug + Send + 'static> TestLoop<Data, Event> {
    pub fn new(pending_events: sync::mpsc::Receiver<Event>, data: Data) -> Self {
        Self { data, events: VecDeque::new(), handlers: Vec::new(), pending_events }
    }

    pub fn add_event(&mut self, event: Event) {
        self.events.push_back(event);
    }

    pub fn register_handler(&mut self, handler: Box<dyn LoopEventHandler<Data, Event>>) {
        self.handlers.push(handler);
    }

    pub fn run(&mut self) {
        'outer: loop {
            while let Ok(event) = self.pending_events.try_recv() {
                self.events.push_back(event);
            }
            if let Some(event) = self.events.pop_front() {
                let mut current_event = event;
                for handler in &mut self.handlers {
                    if let Some(event) = handler.handle(current_event, &mut self.data) {
                        current_event = event;
                    } else {
                        continue 'outer;
                    }
                }
                panic!("Unhandled event: {:?}", current_event);
            }
            // TODO: support timer events
            break;
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

pub struct IndexedLoopEventHandler<InnerData, InnerEvent> {
    handler: Box<dyn LoopEventHandler<InnerData, InnerEvent>>,
}

impl<InnerData, InnerEvent> LoopEventHandler<Vec<InnerData>, (usize, InnerEvent)>
    for IndexedLoopEventHandler<InnerData, InnerEvent>
{
    fn handle(
        &mut self,
        event: (usize, InnerEvent),
        data: &mut Vec<InnerData>,
    ) -> Option<(usize, InnerEvent)> {
        let idx = event.0;
        match self.handler.handle(event.1, &mut data[idx]) {
            Some(event) => Some((idx, event)),
            None => None,
        }
    }
}

pub struct CaptureEvents<CapturedEvent: Debug + Send + 'static> {
    _marker: std::marker::PhantomData<CapturedEvent>,
}

impl<CapturedEvent: Debug + Send + 'static> CaptureEvents<CapturedEvent> {
    pub fn new() -> Self {
        Self { _marker: std::marker::PhantomData }
    }
}

impl<
        CapturedEvent: Debug + Send + 'static,
        Data: AsMut<Vec<CapturedEvent>>,
        Event: TryIntoOrSelf<CapturedEvent>,
    > LoopEventHandler<Data, Event> for CaptureEvents<CapturedEvent>
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

#[derive(Clone)]
pub struct LoopSender<Event: Send + 'static> {
    event_sender: sync::mpsc::SyncSender<Event>,
}

impl<Message: Send + 'static, Event: From<Message> + Send + 'static> messaging::Sender<Message>
    for LoopSender<Event>
{
    fn send(&self, message: Message) {
        self.event_sender.send(message.into()).unwrap();
    }
}

#[derive(Clone)]
pub struct LoopSenderForIndex<Event: Send + 'static> {
    inner_sender: LoopSender<Event>,
    index: usize,
}

impl<Message: Send + 'static> messaging::Sender<Message> for LoopSenderForIndex<(usize, Message)> {
    fn send(&self, message: Message) {
        self.inner_sender.send((self.index, message));
    }
}