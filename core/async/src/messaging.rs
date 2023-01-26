use once_cell::sync::OnceCell;
use std::sync::Arc;

pub trait Sender<M: Send + 'static>: Send + Sync + 'static {
    fn send(&self, message: M);
}

pub type ArcSender<M> = Arc<dyn Sender<M>>;

impl<M, A> Sender<M> for actix::Addr<A>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    A: actix::Actor + actix::Handler<M>,
    A::Context: actix::dev::ToEnvelope<A, M>,
{
    fn send(&self, message: M) {
        actix::Addr::do_send(self, message)
    }
}

pub struct LateBoundSender<S> {
    sender: OnceCell<Arc<S>>,
}

impl<S> Default for LateBoundSender<S> {
    fn default() -> Self {
        Self { sender: OnceCell::default() }
    }
}

impl<S> LateBoundSender<S> {
    pub fn set_sender(&self, sender: Arc<S>) {
        self.sender.set(sender).ok().expect("cannot set sender twice");
    }
}

impl<M: Send + 'static, S: Sender<M>> Sender<M> for LateBoundSender<S> {
    fn send(&self, message: M) {
        self.sender.wait().send(message);
    }
}

pub struct NoopSenderForTest {}

impl NoopSenderForTest {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

impl<M: Send + 'static> Sender<M> for NoopSenderForTest {
    fn send(&self, _message: M) {}
}
