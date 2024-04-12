use std::sync::Arc;

use concurrent_queue::{ConcurrentQueue, PopError, PushError};
use event_listener::Event;

use crate::exchange::{ExchangeMessage, ExchangeWriter};

mod durable;
pub use durable::{Delivery, DurableQueue};

pub enum QueueSize {
  Bounded(usize),
  Unbounded,
}

impl QueueSize {
  fn make_queue<T>(self) -> ConcurrentQueue<T> {
    match self {
      Self::Bounded(size) => ConcurrentQueue::bounded(size),
      Self::Unbounded => ConcurrentQueue::unbounded(),
    }
  }
}

pub struct Queue<T> {
  queue: ConcurrentQueue<T>,
  recv_event: Event,
  send_event: Event,
  close_event: Event,
}

impl<T> Queue<T> {
  pub fn new(size: QueueSize) -> Self {
    Self {
      queue: size.make_queue(),
      recv_event: Event::new(),
      send_event: Event::new(),
      close_event: Event::new(),
    }
  }

  pub fn bounded(cap: usize) -> Self {
    Self::new(QueueSize::Bounded(cap))
  }

  pub fn unbounded() -> Self {
    Self::new(QueueSize::Unbounded)
  }

  pub fn close(&self) -> bool {
    if self.queue.close() {
      self.send_event.notify(usize::MAX);
      self.recv_event.notify(usize::MAX);
      self.close_event.notify(usize::MAX);
      true
    } else {
      false
    }
  }

  pub async fn wait_for_close(self: Arc<Self>) {
    loop {
      if self.queue.is_closed() {
        return;
      }

      self.close_event.listen().await;
    }
  }

  pub fn is_closed(&self) -> bool {
    self.queue.is_closed()
  }

  pub fn len(&self) -> usize {
    self.queue.len()
  }

  pub fn capacity(&self) -> Option<usize> {
    self.queue.capacity()
  }

  pub fn is_empty(&self) -> bool {
    self.queue.is_empty()
  }

  pub fn is_full(&self) -> bool {
    self.queue.is_full()
  }

  pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
    match self.queue.push(value) {
      Ok(_) => {
        self.recv_event.notify(1);
        Ok(())
      }
      Err(PushError::Closed(x)) => Err(TrySendError::Closed(x)),
      Err(PushError::Full(x)) => Err(TrySendError::Full(x)),
    }
  }

  pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
    let mut value = Some(value);
    loop {
      match self.try_send(value.take().unwrap()) {
        Ok(_) => return Ok(()),
        Err(TrySendError::Closed(x)) => return Err(SendError::Closed(x)),
        Err(TrySendError::Full(x)) => value = Some(x),
      }
    }
  }

  pub fn try_recv(&self) -> Result<T, TryRecvError> {
    match self.queue.pop() {
      Ok(x) => {
        self.send_event.notify(1);
        Ok(x)
      }
      Err(PopError::Closed) => Err(TryRecvError::Closed),
      Err(PopError::Empty) => Err(TryRecvError::Empty),
    }
  }

  pub async fn recv(&self) -> Result<T, RecvError> {
    loop {
      match self.try_recv() {
        Ok(x) => return Ok(x),
        Err(TryRecvError::Closed) => return Err(RecvError::Closed),
        Err(TryRecvError::Empty) => self.recv_event.listen().await,
      }
    }
  }
}

#[derive(Debug, Clone)]
pub enum TrySendError<T> {
  Full(T),
  Closed(T),
}

#[derive(Debug, Clone)]
pub enum SendError<T> {
  Closed(T),
}

#[derive(Debug, Clone)]
pub enum TryRecvError {
  Empty,
  Closed,
}

#[derive(Debug, Clone)]
pub enum RecvError {
  Closed,
}

impl<M, T: Send + 'static> ExchangeWriter<M> for Arc<Queue<T>>
where
  T: From<ExchangeMessage<M>>,
{
  fn push(&self, msg: ExchangeMessage<M>) {
    self.try_send(msg.into());
  }
}

impl<'a, M, T: Send + 'static> ExchangeWriter<M> for &'a Queue<T>
where
  T: From<ExchangeMessage<M>>,
{
  fn push(&self, msg: ExchangeMessage<M>) {
    self.try_send(msg.into());
  }
}
