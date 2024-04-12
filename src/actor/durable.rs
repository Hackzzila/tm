#[allow(unused_variables)]
pub trait DurableActor: Sized {
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

pub trait HandleMessageDurable<T>: Actor + Send {
  fn handle_durable(
    &mut self,
    handle: &ActorHandle<Self>,
    message: &T,
  ) -> impl Future<Output = Result<(), BoxError>> + Send;
}

trait DynDurableHandler<A: Actor>: Send {
  fn dyn_handle_durable<'a>(
    &'a self,
    handle: &'a ActorHandle<A>,
    state: &'a mut A,
  ) -> BoxFuture<'a, Result<(), BoxError>>;
  fn as_any(self: Box<Self>) -> Box<dyn Any + Send>;
}

impl<T: Send + 'static, A: HandleMessageDurable<T>> DynDurableHandler<A> for T {
  fn dyn_handle_durable<'a>(
    &'a self,
    handle: &'a ActorHandle<A>,
    state: &'a mut A,
  ) -> BoxFuture<'a, Result<(), BoxError>> {
    state.handle_durable(handle, self).boxed()
  }

  fn as_any(self: Box<Self>) -> Box<dyn Any + Send> {
    self
  }
}

pub type DurableActorQueue<A> = Arc<DurableQueue<ActorQueueMessage<A>>>;
pub struct DurableActorQueueMessage<A: ?Sized>(Box<dyn DynDurableHandler<A>>);

impl<T: Send + 'static, A: HandleMessageDurable<T>> From<ExchangeMessage<T>> for DurableActorQueueMessage<A> {
  fn from(value: ExchangeMessage<T>) -> Self {
    ActorQueueMessage(Box::new(value.0) as Box<_>)
  }
}
