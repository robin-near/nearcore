use std::{sync::Arc, time::Duration};

use crate::messaging;

use super::multi_instance::IndexedDelaySender;

/// Interface to send an event with a delay. It implements Sender for any
/// message that can be converted into this event type.
pub struct DelaySender<Event> {
    // We use an impl object here because it makes the interop with other
    // traits much easier.
    im: Arc<dyn DelaySenderImpl<Event>>,
}

pub(crate) trait DelaySenderImpl<Event>: Send + Sync {
    fn send_with_delay(&self, event: Event, delay: Duration);
}

/// Implements any DelaySenderImpl<Other> as long as Other can be converted into Event.
pub struct NarrowingDelaySender<Event> {
    sender: DelaySender<Event>,
}

impl<Event, OuterEvent: From<Event>> DelaySenderImpl<Event> for NarrowingDelaySender<OuterEvent> {
    fn send_with_delay(&self, event: Event, delay: Duration) {
        self.sender.send_with_delay(event.into(), delay);
    }
}

impl<Event> DelaySender<Event> {
    pub(crate) fn new<Impl: DelaySenderImpl<Event> + 'static>(im: Impl) -> Self {
        Self { im: Arc::new(im) }
    }

    pub fn send_with_delay(&self, event: Event, delay: Duration) {
        self.im.send_with_delay(event, delay);
    }

    /// Converts into a sender that can send any event convertible into this event type.
    pub fn narrow<InnerEvent>(self) -> DelaySender<InnerEvent>
    where
        Event: From<InnerEvent> + 'static,
    {
        DelaySender { im: Arc::new(NarrowingDelaySender { sender: self }) }
    }
}

impl<Event: 'static> DelaySender<(usize, Event)> {
    pub fn for_index(self, index: usize) -> DelaySender<Event> {
        DelaySender { im: Arc::new(IndexedDelaySender::new(self.im, index)) }
    }
}

impl<Event> Clone for DelaySender<Event> {
    fn clone(&self) -> Self {
        Self { im: self.im.clone() }
    }
}

/// Allows DelaySender to be used in contexts where we don't need a delay,
/// such as being given to something that expects an actix recipient.
impl<Message, Event: From<Message> + 'static> messaging::Sender<Message> for DelaySender<Event> {
    fn send(&self, message: Message) {
        self.send_with_delay(message.into(), Duration::ZERO);
    }
}
