use std::borrow::Borrow;

use task::TaskHandle;

pub mod actor;
pub mod exchange;
pub mod queue;
pub mod task;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub trait Spawnable {
  type Handle: Borrow<TaskHandle>;

  fn spawn(self) -> Self::Handle;
}
