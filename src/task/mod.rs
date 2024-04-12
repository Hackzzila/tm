use std::{any::Any, collections::HashSet, panic::AssertUnwindSafe};

use futures::{
  stream::{AbortHandle, Abortable},
  FutureExt,
};
use slab::Slab;
use tracing::{debug, span, trace, Instrument, Level};

use crate::Spawnable;

pub mod handle;
pub mod manager;
pub mod simple;

pub use handle::TaskHandle;
pub use manager::TaskManager;
pub use simple::{SimpleTask, SimpleTaskPool};

#[derive(Debug)]
enum TaskSystemMessage {
  Kill(StopReason),
  Stop(StopReason),
  Panic(usize, Box<dyn Any + Send>),
  ReplicaExited(usize),
  LinkDied(StopReason, TaskHandle),
  AddLink(TaskHandle),
  RemoveLink(TaskHandle),
}

#[derive(Debug, Clone)]
pub enum PanicAction {
  /// Respawn the replica that panicked
  Respawn,

  /// Ignore the panic, will exit if there are no more running replicas
  Ignore,
}

#[derive(Debug, Clone)]
pub enum StopReason {
  Normal,
  Abnormal,
  AllReplicasExited,
  AllReplicasPanicked,
  LinkDied,
}

fn spawn_replica<T: TaskManager>(mgr: &mut T, handle: TaskHandle, replicas: &mut Slab<AbortHandle>) {
  let (abort_handle, abort_registration) = AbortHandle::new_pair();

  let entry = replicas.vacant_entry();
  let id = entry.key();
  entry.insert(abort_handle);

  debug!(id, "spawning replica");

  let task = mgr.spawn(&handle, id);
  let task = AssertUnwindSafe(task).catch_unwind();

  let span = span!(Level::INFO, "spawn_replica", id);

  let future = async move {
    let res = task.await;
    if let Err(err) = res {
      debug!("replica panicked");
      handle.try_send(TaskSystemMessage::Panic(id, err));
    } else {
      debug!("replica exited");
      handle.try_send(TaskSystemMessage::ReplicaExited(id));
    }
  }
  .instrument(span);

  tokio::task::spawn(Abortable::new(future, abort_registration));
}

async fn run_task_manager<T: TaskManager + Send + 'static>(mut mgr: T, handle: TaskHandle) {
  trace!("calling pre_start");
  mgr.pre_start(&handle).await;

  let replica_count = mgr.replicas();

  let mut replicas = Slab::with_capacity(replica_count);

  debug!("spawning '{replica_count}' replicas");

  for _ in 0..replica_count {
    spawn_replica(&mut mgr, handle.clone(), &mut replicas);
  }

  trace!("calling post_start");
  mgr.post_start(&handle).await;

  let mut links = HashSet::new();

  let mut stop_reason = StopReason::Abnormal;
  loop {
    match handle.recv().await {
      Ok(TaskSystemMessage::Kill(reason)) => {
        debug!(?reason, "received kill message");
        stop_reason = reason;

        for abort_handle in replicas.drain() {
          abort_handle.abort();
        }

        break;
      }

      Ok(TaskSystemMessage::Stop(reason)) => {
        debug!(?reason, "received stop message");
        mgr.stop(&handle, reason.clone()).await;
        stop_reason = reason;
        // break;
      }

      Ok(TaskSystemMessage::Panic(id, _err)) => {
        debug!(id, "replica panicked");

        replicas.remove(id).abort();

        match mgr.on_panic(&handle, id).await {
          PanicAction::Respawn => spawn_replica(&mut mgr, handle.clone(), &mut replicas),
          PanicAction::Ignore => {
            trace!("ignoring panic");
            if replicas.is_empty() {
              debug!("all replicas panicked, exiting...");
              stop_reason = StopReason::AllReplicasPanicked;
              break;
            }
          }
        }
      }

      Ok(TaskSystemMessage::AddLink(link_handle)) => {
        links.insert(link_handle);
      }

      Ok(TaskSystemMessage::RemoveLink(link_handle)) => {
        links.remove(&link_handle);
      }

      Ok(TaskSystemMessage::LinkDied(reason, link_handle)) => {
        mgr.link_died(&handle, reason, &link_handle).await;
      }

      Ok(TaskSystemMessage::ReplicaExited(id)) => {
        debug!(id, "replica exited");
        replicas.remove(id);
        if replicas.is_empty() {
          debug!("all replicas exited, exiting...");
          stop_reason = StopReason::AllReplicasExited;
          break;
        }
      }

      Err(_) => break,
    }
  }

  debug!("notifying links about exit");

  for link in links {
    link.try_send(TaskSystemMessage::LinkDied(stop_reason.clone(), handle.clone()));
  }

  mgr.on_exit(&handle, stop_reason).await;

  debug!("goodbye");
}

impl<T: TaskManager + Send + 'static> Spawnable for T {
  type Handle = TaskHandle;
  fn spawn(self) -> Self::Handle {
    let handle = TaskHandle::new();
    let cloned_handle = handle.clone();

    tokio::task::spawn(run_task_manager(self, handle));

    cloned_handle
  }
}
