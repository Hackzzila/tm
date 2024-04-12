use std::{
  any::Any,
  borrow::Cow,
  collections::HashSet,
  sync::{atomic::AtomicU64, Arc},
};

use async_trait::async_trait;
use futures::{future::BoxFuture, stream::AbortHandle};
use slab::Slab;

use crate::{
  queue::DurableQueue,
  task::{PanicAction, StopReason, TaskHandle, TaskManager},
};

enum RuntimeMessage {
  SpawnTask {
    task_id: u64,
    mgr: Box<dyn DynTaskManager>,
  },

  KillTask {
    task_id: u64,
    reason: StopReason,
  },

  StopTask {
    task_id: u64,
    reason: StopReason,
  },

  ReplicaPanicked {
    task_id: u64,
    replica_id: usize,
    panic: Box<dyn Any + Send>,
  },

  ReplicaExited {
    task_id: u64,
    replica_id: usize,
  },

  AddLink {
    parent_task: u64,
    child_task: u64,
  },

  RemoveLink {
    parent_task: u64,
    child_task: u64,
  },
}

pub struct Runtime {}

pub struct Task {
  links: HashSet<u64>,
  replicas: Slab<AbortHandle>,
}

struct RuntimeHandleInner {
  queue: DurableQueue<RuntimeMessage>,
  next_task_id: AtomicU64,
}

pub struct RuntimeHandle(Arc<RuntimeHandleInner>);

#[async_trait]
#[allow(unused_variables)]
pub trait DynTaskManager {
  fn spawn(&mut self, handle: &TaskHandle, replica: usize) -> BoxFuture<'static, ()>;
  async fn stop(&mut self, handle: &TaskHandle, reason: StopReason);

  fn name(&self) -> Cow<'static, str> {
    std::any::type_name::<Self>().into()
  }

  fn replicas(&self) -> usize {
    1
  }

  async fn pre_start(&mut self, handle: &TaskHandle) {}

  async fn post_start(&mut self, handle: &TaskHandle) {}

  async fn on_exit(&mut self, handle: &TaskHandle, reason: StopReason) {}

  async fn on_panic(&mut self, handle: &TaskHandle, replica: usize) -> PanicAction {
    PanicAction::Ignore
  }

  fn link_died(&mut self, handle: &TaskHandle, reason: StopReason, link_handle: &TaskHandle) {
    handle.stop(StopReason::LinkDied);
  }
}
