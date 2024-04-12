use std::{ops::Deref, sync::Arc};

use concurrent_queue::{ConcurrentQueue, PopError, PushError};
use event_listener::Event;
use tracing::error;

use super::{QueueSize, RecvError, SendError, TryRecvError, TrySendError};
use crate::exchange::{ExchangeMessage, ExchangeWriter};

pub struct DurableQueue<T> {
  queue: ConcurrentQueue<T>,
  retries: ConcurrentQueue<Delivery<T>>,
  retry_count: usize,
  recv_event: Event,
  send_event: Event,
  close_event: Event,
}

pub struct Delivery<T> {
  msg: Option<T>,
  retry_count: usize,
  queue: Arc<DurableQueue<T>>,
}

impl<T> Delivery<T> {
  pub fn ack(mut self) -> T {
    self.msg.take().unwrap()
  }
}

impl<T> Deref for Delivery<T> {
  type Target = T;
  fn deref(&self) -> &Self::Target {
    self.msg.as_ref().unwrap()
  }
}

impl<T> Drop for Delivery<T> {
  fn drop(&mut self) {
    self.retry_count -= 1;
    if self.retry_count == 0 {
      return;
    }

    if let Some(msg) = self.msg.take() {
      let res = self.queue.retries.push(Delivery {
        msg: Some(msg),
        retry_count: self.retry_count,
        queue: self.queue.clone(),
      });

      if res.is_ok() {
        self.queue.recv_event.notify(1);
      } else {
        error!("failed to push retry into queue");
      }
    }
  }
}

impl<T> DurableQueue<T> {
  pub fn new(retry_count: usize, queue_size: QueueSize, retry_queue_size: QueueSize) -> Self {
    Self {
      queue: queue_size.make_queue(),
      retries: retry_queue_size.make_queue(),
      retry_count,
      recv_event: Event::new(),
      send_event: Event::new(),
      close_event: Event::new(),
    }
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

  pub fn try_recv_non_durable(&self) -> Result<T, TryRecvError> {
    match self.queue.pop() {
      Ok(x) => {
        self.send_event.notify(1);
        Ok(x)
      }
      Err(PopError::Closed) => Err(TryRecvError::Closed),
      Err(PopError::Empty) => Err(TryRecvError::Empty),
    }
  }

  pub async fn recv_non_durable(&self) -> Result<T, RecvError> {
    loop {
      match self.try_recv_non_durable() {
        Ok(x) => return Ok(x),
        Err(TryRecvError::Closed) => return Err(RecvError::Closed),
        Err(TryRecvError::Empty) => self.recv_event.listen().await,
      }
    }
  }

  pub fn try_recv(self: Arc<Self>) -> Result<Delivery<T>, TryRecvError> {
    match self.retries.pop() {
      Ok(x) => Ok(x),
      Err(PopError::Closed) => Err(TryRecvError::Closed),
      Err(PopError::Empty) => match self.try_recv_non_durable() {
        Ok(x) => Ok(Delivery {
          msg: Some(x),
          retry_count: self.retry_count,
          queue: self,
        }),
        Err(e) => Err(e),
      },
    }
  }

  pub async fn recv(self: Arc<Self>) -> Result<Delivery<T>, RecvError> {
    loop {
      match self.clone().try_recv() {
        Ok(x) => return Ok(x),
        Err(TryRecvError::Closed) => return Err(RecvError::Closed),
        Err(TryRecvError::Empty) => self.recv_event.listen().await,
      }
    }
  }
}

impl<M, T: Send + 'static> ExchangeWriter<M> for Arc<DurableQueue<T>>
where
  T: From<ExchangeMessage<M>>,
{
  fn push(&self, msg: ExchangeMessage<M>) {
    self.clone().try_send(msg.into());
  }
}
