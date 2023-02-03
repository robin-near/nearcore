use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use near_o11y::{WithSpanContext, WithSpanContextExt};
use once_cell::sync::OnceCell;
use std::sync::Arc;

pub trait CanSend<M>: Send + Sync + 'static {
    fn send(&self, message: M);
}

pub trait IntoSender<M> {
    fn into_sender(self) -> Sender<M>;
}

impl<M, T: CanSend<M>> IntoSender<M> for T {
    fn into_sender(self) -> Sender<M> {
        Sender::from_impl(self)
    }
}

pub trait ArcIntoSender<M> {
    fn into_sender(self: Arc<Self>) -> Sender<M>;
}

impl<M, T: CanSend<M>> ArcIntoSender<M> for T {
    fn into_sender(self: Arc<Self>) -> Sender<M> {
        Sender::from_arc(self)
    }
}

impl<A: AsRef<Sender<M>> + Send + Sync + 'static, M: 'static> CanSend<M> for A {
    fn send(&self, message: M) {
        self.as_ref().send(message)
    }
}

pub struct Sender<M: 'static> {
    sender: Arc<dyn CanSend<M>>,
}

impl<M> Clone for Sender<M> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone() }
    }
}

impl<M> Sender<M> {
    fn from_impl(sender: impl CanSend<M> + 'static) -> Self {
        Self { sender: Arc::new(sender) }
    }

    fn from_arc<T: CanSend<M> + 'static>(arc: Arc<T>) -> Self {
        Self { sender: arc }
    }

    pub fn noop() -> Self {
        Self::from_impl(NoopCanSend)
    }

    pub fn send(&self, message: M) {
        self.sender.send(message)
    }
}

pub struct SenderWithSpanContext<M: actix::Message> {
    inner: Arc<dyn CanSend<WithSpanContext<M>>>,
}

impl<M: actix::Message> Sender<WithSpanContext<M>> {
    pub fn as_sender_with_span_context(&self) -> Sender<M> {
        Sender::from_impl(SenderWithSpanContext { inner: self.sender.clone() })
    }
}

impl<M: actix::Message + 'static> CanSend<M> for SenderWithSpanContext<M> {
    fn send(&self, message: M) {
        self.inner.send(message.with_span_context())
    }
}

impl<M, A> CanSend<M> for actix::Addr<A>
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

pub trait CanSendAsync<M, R>: Send + Sync + 'static {
    fn send_async(&self, message: M) -> BoxFuture<'static, R>;
}

pub struct AsyncSender<M: 'static, R: 'static> {
    sender: Arc<dyn CanSendAsync<M, R>>,
}

impl<M, R> Clone for AsyncSender<M, R> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone() }
    }
}

pub trait IntoAsyncSender<M, R> {
    fn into_async_sender(self) -> AsyncSender<M, R>;
}

impl<M, R, T: CanSendAsync<M, R>> IntoAsyncSender<M, R> for T {
    fn into_async_sender(self) -> AsyncSender<M, R> {
        AsyncSender::from_impl(self)
    }
}

pub trait ArcIntoAsyncSender<M, R> {
    fn into_async_sender(self: Arc<Self>) -> AsyncSender<M, R>;
}

impl<M, R, T: CanSendAsync<M, R>> ArcIntoAsyncSender<M, R> for T {
    fn into_async_sender(self: Arc<Self>) -> AsyncSender<M, R> {
        AsyncSender::from_arc(self)
    }
}

impl<A: AsRef<AsyncSender<M, R>> + Send + Sync + 'static, M: 'static, R: 'static> CanSendAsync<M, R>
    for A
{
    fn send_async(&self, message: M) -> BoxFuture<'static, R> {
        self.as_ref().send_async(message)
    }
}

impl<M, R> AsyncSender<M, R> {
    fn from_impl(sender: impl CanSendAsync<M, R> + 'static) -> Self {
        Self { sender: Arc::new(sender) }
    }

    fn from_arc<T: CanSendAsync<M, R> + 'static>(arc: Arc<T>) -> Self {
        Self { sender: arc }
    }

    pub fn send_async(&self, message: M) -> BoxFuture<'static, R> {
        self.sender.send_async(message)
    }
}

impl<M, A> CanSendAsync<M, Result<M::Result, ()>> for actix::Addr<A>
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

pub struct AsyncSenderWithSpanContext<M: actix::Message, R> {
    inner: Arc<dyn CanSendAsync<WithSpanContext<M>, R>>,
}

impl<M: actix::Message, R> AsyncSender<WithSpanContext<M>, R> {
    pub fn as_sender_with_span_context(&self) -> AsyncSender<M, R> {
        AsyncSender::from_impl(AsyncSenderWithSpanContext { inner: self.sender.clone() })
    }
}

impl<M: actix::Message + 'static, R: 'static> CanSendAsync<M, R>
    for AsyncSenderWithSpanContext<M, R>
{
    fn send_async(&self, message: M) -> BoxFuture<'static, R> {
        self.inner.send_async(message.with_span_context())
    }
}

pub struct LateBoundSender<S> {
    sender: OnceCell<S>,
}

impl<S> Default for LateBoundSender<S> {
    fn default() -> Self {
        Self { sender: OnceCell::default() }
    }
}

impl<S> LateBoundSender<S> {
    pub fn bind(&self, sender: S) {
        self.sender.set(sender).map_err(|_| ()).expect("cannot set sender twice");
    }
}

impl<M, S: CanSend<M>> CanSend<M> for LateBoundSender<S> {
    fn send(&self, message: M) {
        self.sender.wait().send(message);
    }
}

impl<M, R, S: CanSendAsync<M, R>> CanSendAsync<M, R> for LateBoundSender<S> {
    fn send_async(&self, message: M) -> BoxFuture<'static, R> {
        self.sender.wait().send_async(message)
    }
}

struct NoopCanSend;

impl<M> CanSend<M> for NoopCanSend {
    fn send(&self, _message: M) {}
}
