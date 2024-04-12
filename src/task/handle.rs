use std::{borrow::Borrow, fmt::Debug, hash::Hash, sync::Arc};

use super::{StopReason, TaskSystemMessage};
use crate::{
  queue::{Queue, RecvError},
  Spawnable,
};

struct TaskHandleInner {
  queue: Queue<TaskSystemMessage>,
}

#[derive(Clone)]
pub struct TaskHandle(Arc<TaskHandleInner>);

impl TaskHandle {
  pub(super) fn new() -> Self {
    Self(Arc::new(TaskHandleInner {
      queue: Queue::unbounded(),
    }))
  }

  pub(super) fn try_send(&self, msg: TaskSystemMessage) {
    let _ = self.0.queue.try_send(msg);
  }

  pub(super) async fn recv(&self) -> Result<TaskSystemMessage, RecvError> {
    self.0.queue.recv().await
  }

  pub fn stop(&self, reason: StopReason) {
    self.try_send(TaskSystemMessage::Stop(reason));
  }

  pub fn kill(&self, reason: StopReason) {
    self.try_send(TaskSystemMessage::Kill(reason));
  }

  pub fn link_child(&self, child: TaskHandle) {
    self.try_send(TaskSystemMessage::AddLink(child));
  }

  pub fn unlink_child(&self, child: TaskHandle) {
    self.try_send(TaskSystemMessage::RemoveLink(child));
  }

  pub fn link_sibling(&self, sibling: TaskHandle) {
    self.link_child(sibling.clone());
    sibling.link_child(self.clone());
  }

  pub fn unlink_sibling(&self, sibling: TaskHandle) {
    self.unlink_child(sibling.clone());
    sibling.unlink_child(self.clone());
  }

  pub fn spawn_child<S: Spawnable>(&self, child: S) -> S::Handle {
    let child_handle = child.spawn();
    let child_task_handle: &TaskHandle = child_handle.borrow();
    self.link_child(child_task_handle.clone());
    child_handle
  }

  pub fn spawn_sibling<S: Spawnable>(&self, sibling: S) -> S::Handle {
    let sibling_handle = sibling.spawn();
    let sibling_task_handle: &TaskHandle = sibling_handle.borrow();
    self.link_sibling(sibling_task_handle.clone());
    sibling_handle
  }
}

impl Hash for TaskHandle {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    Arc::as_ptr(&self.0).hash(state)
  }
}

impl Eq for TaskHandle {}

impl PartialEq<Self> for TaskHandle {
  fn eq(&self, other: &Self) -> bool {
    Arc::ptr_eq(&self.0, &other.0)
  }
}

impl Debug for TaskHandle {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "TaskHandle")
  }
}
