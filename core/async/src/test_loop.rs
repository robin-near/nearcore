use std::{collections::VecDeque, fmt::Debug, sync};

use crate::messaging;

pub struct TestLoop<Data, Event: Debug + Send + 'static> {
    data: Data,
    events: VecDeque<Event>,
    handlers: Vec<Box<dyn LoopEventHandler<Data, Event>>>,
    pending_events: sync::mpsc::Receiver<Event>,
    pending_events_sender: sync::mpsc::SyncSender<Event>,
}

pub trait LoopEventHandler<Data, Event> {
    fn handle(&mut self, event: Event, data: &mut Data) -> Option<Event>;
}

impl<Data, Event: Debug + Send + 'static> TestLoop<Data, Event> {
    pub fn new(data: Data) -> Self {
        let (pending_events_sender, pending_events) = sync::mpsc::sync_channel(65536);
        Self {
            data,
            events: VecDeque::new(),
            handlers: Vec::new(),
            pending_events,
            pending_events_sender,
        }
    }

    pub fn event_sender(&self) -> SendsToLoop<Event> {
        SendsToLoop { event_sender: self.pending_events_sender.clone() }
    }

    pub fn event_sender_for_index(&self, index: usize) -> SendsToLoopForIndex<Event> {
        SendsToLoopForIndex { inner_sender: self.event_sender(), index }
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

pub trait HasIndexedData<InnerData> {
    fn for_index(&mut self, index: usize) -> &mut InnerData;
}

pub struct ForwardToIndex<InnerData, InnerEvent> {
    inner_handler: Box<dyn LoopEventHandler<InnerData, InnerEvent>>,
}

impl<
        InnerData,
        Data: HasIndexedData<InnerData>,
        InnerEvent,
        Event: TryIntoOrSelf<(usize, InnerEvent)> + From<(usize, InnerEvent)>,
    > LoopEventHandler<Data, Event> for ForwardToIndex<InnerData, InnerEvent>
{
    fn handle(&mut self, event: Event, data: &mut Data) -> Option<Event> {
        match event.try_into_or_self() {
            Ok((index, inner_event)) => {
                match self.inner_handler.handle(inner_event, data.for_index(index)) {
                    Some(event) => Some((index, event).into()),
                    None => None,
                }
            }
            Err(event) => Some(event),
        }
    }
}

#[derive(Clone)]
pub struct SendsToLoop<Event: Send + 'static> {
    event_sender: sync::mpsc::SyncSender<Event>,
}

impl<Message: Send + 'static, Event: From<Message> + Send + 'static> messaging::Sender<Message>
    for SendsToLoop<Event>
{
    fn send(&self, message: Message) {
        self.event_sender.send(message.into()).unwrap();
    }
}

#[derive(Clone)]
pub struct SendsToLoopForIndex<Event: Send + 'static> {
    inner_sender: SendsToLoop<Event>,
    index: usize,
}

impl<Message: Send + 'static, Event: From<(usize, Message)> + Send + 'static>
    messaging::Sender<Message> for SendsToLoopForIndex<Event>
{
    fn send(&self, message: Message) {
        self.inner_sender.send((self.index, message).into());
    }
}
