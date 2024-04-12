use std::{
  any::Any,
  borrow::{Borrow, Cow},
  future::Future,
  ops::Deref,
  panic::AssertUnwindSafe,
  sync::Arc,
};

use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use tracing::{debug, trace};

use crate::{
  exchange::ExchangeMessage,
  queue::{Queue, SendError},
  task::{PanicAction, StopReason, TaskHandle, TaskManager},
  Spawnable,
};

// mod durable;

#[allow(unused_variables)]
pub trait Actor: Sized {
  fn name(&self) -> Cow<'static, str> {
    std::any::type_name::<Self>().into()
  }

  fn pre_start(handle: &ActorHandle<Self>) -> impl Future<Output = ()> + Send {
    std::future::ready(())
  }

  fn on_start(&mut self, handle: &ActorHandle<Self>) -> impl Future<Output = ()> + Send {
    std::future::ready(())
  }

  fn on_stop(self, handle: &ActorHandle<Self>) -> impl Future<Output = ()> + Send {
    std::future::ready(())
  }
}

pub trait HandleMessage<T>: Actor + Send {
  fn handle(&mut self, handle: &ActorHandle<Self>, message: T) -> impl Future<Output = ()> + Send;
}

trait DynHandler<A: Actor>: Send {
  fn dyn_handle<'a>(self: Box<Self>, handle: &'a ActorHandle<A>, state: &'a mut A) -> BoxFuture<'a, ()>;
  fn as_any(self: Box<Self>) -> Box<dyn Any + Send>;
}

impl<T: Send + 'static, A: HandleMessage<T>> DynHandler<A> for T {
  fn dyn_handle<'a>(self: Box<Self>, handle: &'a ActorHandle<A>, state: &'a mut A) -> BoxFuture<'a, ()> {
    state.handle(handle, *self).boxed()
  }

  fn as_any(self: Box<Self>) -> Box<dyn Any + Send> {
    self
  }
}

pub type ActorQueue<A> = Arc<Queue<ActorQueueMessage<A>>>;
pub struct ActorQueueMessage<A: ?Sized>(Box<dyn DynHandler<A>>);

impl<T: Send + 'static, A: HandleMessage<T>> From<ExchangeMessage<T>> for ActorQueueMessage<A> {
  fn from(value: ExchangeMessage<T>) -> Self {
    ActorQueueMessage(Box::new(value.0) as Box<_>)
  }
}

struct SingleActorManager<A: Actor> {
  name: Cow<'static, str>,
  actor: Option<A>,
  queue: ActorQueue<A>,
}

impl<A: Actor + Send + 'static> TaskManager for SingleActorManager<A> {
  fn replicas(&self) -> usize {
    1
  }

  fn name(&self) -> std::borrow::Cow<'static, str> {
    self.name.clone()
  }

  fn spawn(&mut self, handle: &TaskHandle, _replica: usize) -> impl Future<Output = ()> + Send + 'static {
    let actor = self.actor.take().unwrap();
    let handle = ActorHandle {
      task: handle.clone(),
      queue: self.queue.clone(),
    };

    async move {
      A::pre_start(&handle).await;
      actor_lifecycle(actor, handle).await
    }
  }

  async fn stop(&mut self, _handle: &TaskHandle, _reason: StopReason) {
    self.queue.close();
  }

  async fn on_panic(&mut self, _handle: &TaskHandle, _replica: usize) -> PanicAction {
    PanicAction::Ignore
  }
}

pub struct ActorPool<A, F: Fn() -> A + Send + 'static> {
  replicas: usize,
  spawn_fn: F,
  queue: ActorQueue<A>,
}

impl<A, F: Fn() -> A + Send + 'static> ActorPool<A, F> {
  pub fn new(replicas: usize, spawn_fn: F) -> Self {
    Self::with_queue(replicas, spawn_fn, Arc::new(Queue::unbounded()))
  }
  pub fn with_queue(replicas: usize, spawn_fn: F, queue: ActorQueue<A>) -> Self {
    Self {
      replicas,
      spawn_fn,
      queue,
    }
  }
}

impl<A: Actor + Send + 'static, F: Fn() -> A + Send + 'static> Spawnable for ActorPool<A, F> {
  type Handle = ActorHandle<A>;
  fn spawn(self) -> Self::Handle {
    let mgr = ActorPoolManager {
      spawn_fn: self.spawn_fn,
      replicas: self.replicas,
      queue: self.queue.clone(),
    };

    ActorHandle {
      task: mgr.spawn(),
      queue: self.queue,
    }
  }
}

pub struct SingleActor<A: Actor> {
  actor: A,
  queue: ActorQueue<A>,
}

impl<A: Actor> SingleActor<A> {
  pub fn new(actor: A) -> Self {
    Self::with_queue(actor, Arc::new(Queue::unbounded()))
  }

  pub fn with_queue(actor: A, queue: ActorQueue<A>) -> Self {
    Self { actor, queue }
  }
}

impl<A: Actor + Send + 'static> Spawnable for SingleActor<A> {
  type Handle = ActorHandle<A>;
  fn spawn(self) -> Self::Handle {
    let mgr = SingleActorManager {
      name: self.actor.name(),
      actor: Some(self.actor),
      queue: self.queue.clone(),
    };

    ActorHandle {
      task: mgr.spawn(),
      queue: self.queue,
    }
  }
}

pub struct ActorHandle<A: Actor> {
  pub task: TaskHandle,
  pub queue: ActorQueue<A>,
}

impl<A: Actor> Clone for ActorHandle<A> {
  fn clone(&self) -> Self {
    Self {
      task: self.task.clone(),
      queue: self.queue.clone(),
    }
  }
}

impl<A: Actor> ActorHandle<A> {
  pub fn attach_stream<T, S>(self, stream: S)
  where
    T: Send + 'static,
    A: HandleMessage<T> + 'static,
    S: Stream<Item = T> + Unpin + Send + 'static,
  {
    let queue_clone = self.queue.clone();
    let mut stream = Box::pin(stream.take_until(queue_clone.wait_for_close()));
    tokio::task::spawn(async move {
      while let Some(msg) = stream.next().await {
        if self.send(msg).await.is_err() {
          return;
        }
      }
    });
  }

  pub fn attach_stream_factory<T, S, F>(self, factory: F)
  where
    T: Send + 'static,
    A: HandleMessage<T> + 'static,
    S: Stream<Item = T> + Unpin + Send + 'static,
    F: Fn() -> S + Send + 'static,
    for<'a> &'a F: Send,
  {
    tokio::task::spawn(async move {
      loop {
        let future = AssertUnwindSafe(async {
          let stream = factory();
          let mut stream = Box::pin(stream.take_until(self.queue.clone().wait_for_close()));
          while let Some(msg) = stream.next().await {
            if self.send(msg).await.is_err() {
              return;
            }
          }
        })
        .catch_unwind();

        if future.await.is_ok() {
          return;
        }
      }
    });
  }

  pub async fn send<T>(&self, msg: T) -> Result<(), SendError<T>>
  where
    T: Send + 'static,
    A: HandleMessage<T> + 'static,
  {
    match self.queue.send(ActorQueueMessage(Box::new(msg) as Box<_>)).await {
      Ok(x) => Ok(x),
      Err(SendError::Closed(x)) => Err(SendError::Closed(*x.0.as_any().downcast().unwrap())),
    }
  }
}

impl<A: Actor> Borrow<TaskHandle> for ActorHandle<A> {
  fn borrow(&self) -> &TaskHandle {
    &self.task
  }
}

impl<A: Actor> Deref for ActorHandle<A> {
  type Target = TaskHandle;
  fn deref(&self) -> &Self::Target {
    &self.task
  }
}

struct ActorPoolManager<A: Actor, F: Fn() -> A> {
  spawn_fn: F,
  replicas: usize,
  queue: ActorQueue<A>,
}

impl<A: Actor + Send + 'static, F: Fn() -> A + Send> TaskManager for ActorPoolManager<A, F> {
  fn replicas(&self) -> usize {
    self.replicas
  }

  fn name(&self) -> Cow<'static, str> {
    format!("ActorPoolManager<{}>", std::any::type_name::<A>()).into()
  }

  async fn pre_start(&mut self, handle: &TaskHandle) {
    let handle = ActorHandle {
      task: handle.clone(),
      queue: self.queue.clone(),
    };

    A::pre_start(&handle).await;
  }

  fn spawn(&mut self, handle: &TaskHandle, _replica: usize) -> impl Future<Output = ()> + Send + 'static {
    let handle = ActorHandle {
      task: handle.clone(),
      queue: self.queue.clone(),
    };

    actor_lifecycle((self.spawn_fn)(), handle)
  }

  async fn stop(&mut self, _handle: &TaskHandle, _reason: StopReason) {
    self.queue.close();
  }

  async fn on_panic(&mut self, _handle: &TaskHandle, _replica: usize) -> PanicAction {
    PanicAction::Respawn
  }
}

async fn actor_lifecycle<A: Actor + Send + 'static>(mut actor: A, handle: ActorHandle<A>) {
  trace!("calling on_start hook");
  actor.on_start(&handle).await;

  while let Ok(msg) = handle.queue.clone().recv().await {
    msg.0.dyn_handle(&handle, &mut actor).await;
  }

  actor.on_stop(&handle).await;
  debug!("goodbye");
}
