use std::future::Future;

use super::{PanicAction, StopReason, TaskHandle, TaskManager};

pub struct SimpleTask<F: Future<Output = ()> + Send + 'static> {
  future: Option<F>,
}

impl<F: Future<Output = ()> + Send + 'static> SimpleTask<F> {
  pub fn new(future: F) -> Self {
    Self { future: Some(future) }
  }
}

impl<F: Future<Output = ()> + Send + 'static> TaskManager for SimpleTask<F> {
  fn replicas(&self) -> usize {
    1
  }

  fn spawn(&mut self, _handle: &TaskHandle, _replica: usize) -> impl Future<Output = ()> + Send + 'static {
    self.future.take().unwrap()
  }

  async fn stop(&mut self, handle: &TaskHandle, reason: StopReason) {
    handle.kill(reason);
  }

  async fn on_panic(&mut self, _handle: &TaskHandle, _replica: usize) -> PanicAction {
    PanicAction::Ignore
  }
}

pub struct SimpleTaskPool<S, F>
where
  S: Fn() -> F + Send + 'static,
  F: Future<Output = ()> + Send + 'static,
{
  spawn_fn: S,
  replicas: usize,
}
impl<S, F> SimpleTaskPool<S, F>
where
  S: Fn() -> F + Send + 'static,
  F: Future<Output = ()> + Send + 'static,
{
  pub fn new(replicas: usize, spawn_fn: S) -> Self {
    Self { replicas, spawn_fn }
  }
}

impl<S, F> TaskManager for SimpleTaskPool<S, F>
where
  S: Fn() -> F + Send + 'static,
  F: Future<Output = ()> + Send + 'static,
{
  fn replicas(&self) -> usize {
    self.replicas
  }

  fn spawn(&mut self, _handle: &TaskHandle, _replica: usize) -> impl Future<Output = ()> + Send + 'static {
    (self.spawn_fn)()
  }

  async fn stop(&mut self, handle: &TaskHandle, reason: StopReason) {
    handle.kill(reason);
  }

  async fn on_panic(&mut self, _handle: &TaskHandle, _replica: usize) -> PanicAction {
    PanicAction::Respawn
  }
}
