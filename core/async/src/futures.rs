use futures::{future::BoxFuture, FutureExt};

pub trait FutureSpawner {
    fn spawn_boxed(&self, description: &'static str, f: BoxFuture<'static, ()>);
}

impl<'a> dyn FutureSpawner + 'a {
    pub fn spawn<F>(&self, description: &'static str, f: F)
    where
        F: futures::Future<Output = ()> + Send + 'static,
    {
        self.spawn_boxed(description, f.boxed())
    }
}

pub struct ActixFutureSpawner;

impl FutureSpawner for ActixFutureSpawner {
    fn spawn_boxed(&self, description: &'static str, f: BoxFuture<'static, ()>) {
        near_performance_metrics::actix::spawn(description, f);
    }
}
