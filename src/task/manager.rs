use std::{borrow::Cow, future::Future};

use super::{PanicAction, StopReason, TaskHandle};

#[allow(unused_variables)]
pub trait TaskManager: Sized {
  fn spawn(&mut self, handle: &TaskHandle, replica: usize) -> impl Future<Output = ()> + Send + 'static;
  fn stop(&mut self, handle: &TaskHandle, reason: StopReason) -> impl Future<Output = ()> + Send;

  fn name(&self) -> Cow<'static, str> {
    std::any::type_name::<Self>().into()
  }

  fn replicas(&self) -> usize {
    1
  }

  fn pre_start(&mut self, handle: &TaskHandle) -> impl Future<Output = ()> + Send {
    std::future::ready(())
  }

  fn post_start(&mut self, handle: &TaskHandle) -> impl Future<Output = ()> + Send {
    std::future::ready(())
  }

  fn on_exit(self, handle: &TaskHandle, reason: StopReason) -> impl Future<Output = ()> + Send {
    std::future::ready(())
  }

  fn on_panic(&mut self, handle: &TaskHandle, replica: usize) -> impl Future<Output = PanicAction> + Send {
    std::future::ready(PanicAction::Ignore)
  }

  fn link_died(
    &mut self,
    handle: &TaskHandle,
    reason: StopReason,
    link_handle: &TaskHandle,
  ) -> impl Future<Output = ()> + Send {
    handle.stop(StopReason::LinkDied);
    std::future::ready(())
  }
}
