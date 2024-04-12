use async_lock::RwLock;

#[derive(Debug, Clone)]
pub struct ExchangeMessage<T>(pub T);

pub struct Exchange<'a, T: Clone> {
  writers: RwLock<Vec<Box<dyn ExchangeWriter<T> + 'a>>>,
}

impl<'a, T: Clone + Send + Sync> Exchange<'a, T> {
  pub async fn send(&self, value: T) {
    let msg = ExchangeMessage(value);

    for writer in self.writers.read().await.iter() {
      writer.push(msg.clone());
    }
  }
}

impl<'a, T: Clone> Exchange<'a, T> {
  pub const fn new() -> Self {
    Self {
      writers: RwLock::new(Vec::new()),
    }
  }

  pub async fn bind(&self, writer: impl ExchangeWriter<T> + 'a) {
    self.bind_boxed(Box::new(writer)).await
  }

  pub async fn bind_boxed(&self, writer: Box<dyn ExchangeWriter<T> + 'a>) {
    let writers = &mut *self.writers.write().await;
    writers.push(writer);
  }
}

pub trait ExchangeWriter<T>: Send + Sync {
  fn push(&self, msg: ExchangeMessage<T>);
}
