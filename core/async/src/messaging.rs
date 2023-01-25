use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use near_o11y::{WithSpanContext, WithSpanContextExt};
use once_cell::sync::OnceCell;
use std::sync::Arc;

pub trait Sender<M>: Send + Sync + 'static {
    fn send(&self, message: M);
}

pub type ArcSender<M> = Arc<dyn Sender<M>>;

pub struct SenderWithSpanContext<T> {
    sender: Arc<T>,
}

pub trait SenderWithSpanContextExt<T> {
    fn as_sender_with_span_context(self) -> Arc<SenderWithSpanContext<T>>;
}

impl<T> SenderWithSpanContextExt<T> for Arc<T> {
    fn as_sender_with_span_context(self) -> Arc<SenderWithSpanContext<T>> {
        Arc::new(SenderWithSpanContext { sender: self })
    }
}

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

impl<M: actix::Message, T: Sender<WithSpanContext<M>>> Sender<M> for SenderWithSpanContext<T> {
    fn send(&self, message: M) {
        self.sender.send(message.with_span_context())
    }
}

pub trait AsyncSender<M, R>: Send + Sync + 'static {
    fn send_async(&self, message: M) -> BoxFuture<'static, R>;
}

pub type ArcAsyncSender<M, R> = Arc<dyn AsyncSender<M, R>>;

impl<M, A> AsyncSender<M, Result<M::Result, ()>> for actix::Addr<A>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    A: actix::Actor + actix::Handler<M>,
    A::Context: actix::dev::ToEnvelope<A, M>,
{
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<M::Result, ()>> {
        self.send(message).map_err(|_| ()).boxed()
    }
}

impl<M: actix::Message, T: AsyncSender<WithSpanContext<M>, Result<M::Result, ()>>>
    AsyncSender<M, Result<M::Result, ()>> for SenderWithSpanContext<T>
{
    fn send_async(&self, message: M) -> BoxFuture<'static, Result<M::Result, ()>> {
        self.sender.send_async(message.with_span_context())
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

impl<M, S: Sender<M>> Sender<M> for LateBoundSender<S> {
    fn send(&self, message: M) {
        self.sender.wait().send(message);
    }
}

impl<M, R, S: AsyncSender<M, R>> AsyncSender<M, R> for LateBoundSender<S> {
    fn send_async(&self, message: M) -> BoxFuture<'static, R> {
        self.sender.wait().send_async(message)
    }
}

pub struct NoopSenderForTest {}

impl NoopSenderForTest {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

impl<M> Sender<M> for NoopSenderForTest {
    fn send(&self, _message: M) {}
}

impl<M, R> AsyncSender<M, Result<R, ()>> for NoopSenderForTest {
    fn send_async(&self, _message: M) -> BoxFuture<'static, Result<R, ()>> {
        async { Err(()) }.boxed()
    }
}
