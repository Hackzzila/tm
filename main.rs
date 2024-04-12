use std::{future::Future, sync::Arc, time::Duration};

use rbus::{
  actor::{Actor, ActorHandle, ActorPool, HandleMessage, HandleMessageDurable},
  exchange::{Exchange, ExchangeMessage},
  queue::{DurableQueue, Queue, QueueSize},
  spawn::Spawnable,
  task::{PanicAction, StopReason, TaskHandle, TaskManager},
  BoxError,
};
use tokio::{
  select,
  time::{interval, Instant},
};
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;
use tracing::info;

static INT_EXCHANGE: Exchange<i32> = Exchange::new();
static STRING_EXCHANGE: Exchange<String> = Exchange::new();

#[derive(Debug)]
enum QueueMessages {
  String(String),
  Int(i32),
}

impl From<ExchangeMessage<i32>> for QueueMessages {
  fn from(value: ExchangeMessage<i32>) -> Self {
    Self::Int(value.0)
  }
}

impl From<ExchangeMessage<String>> for QueueMessages {
  fn from(value: ExchangeMessage<String>) -> Self {
    Self::String(value.0)
  }
}

#[derive(Debug)]
struct MyManager {
  token: CancellationToken,
}

impl TaskManager for MyManager {
  fn replicas(&self) -> usize {
    3
  }

  async fn pre_start(&mut self, _handle: &TaskHandle) {
    println!("pre_start");
  }

  fn spawn(&mut self, _handle: &TaskHandle, replica: usize) -> impl Future<Output = ()> + Send + 'static {
    let token = self.token.clone();
    async move {
      let mut interval = tokio::time::interval(Duration::from_secs(1));
      let mut i = 0;
      loop {
        select! {
          _ = interval.tick() => {
            println!("tick from {replica}");
            // i += 1;
            // if i == 3 {
            //   panic!("uh oh");
            // }
          },

          _ = token.cancelled() => {
            println!("graceful shutdown from {replica}");
            return
          }
        }
      }
    }
  }

  async fn post_start(&mut self, _handle: &TaskHandle) {
    println!("post_start");
  }

  async fn stop(&mut self, _handle: &TaskHandle, reason: StopReason) {
    println!("requesting stop: {reason:?}");
    self.token.cancel();
  }

  async fn on_exit(self, _handle: &TaskHandle, reason: StopReason) {
    println!("on_exit: {reason:?}");
  }

  async fn on_panic(&mut self, _handle: &TaskHandle, _replica: usize) -> PanicAction {
    println!("task panicked");
    PanicAction::Ignore
  }

  // async fn link_died(&mut self, handle: &TaskHandleRef, reason: StopReason, link_handle: &TaskHandleRef) {
  //   println!("link died :(");
  // }
}

struct MyActor {
  count: usize,
}

impl Actor for MyActor {
  async fn pre_start(handle: &ActorHandle<Self>) {
    INT_EXCHANGE.bind(handle.queue.clone()).await;
    STRING_EXCHANGE.bind(handle.queue.clone()).await;
  }

  async fn on_stop(self, handle: &ActorHandle<Self>) {
    println!("stopped actor");
  }
}

impl HandleMessageDurable<i32> for MyActor {
  async fn handle_durable(&mut self, _handle: &ActorHandle<Self>, message: &i32) -> Result<(), BoxError> {
    info!("received message: {message} total: {}", self.count);

    self.count += 1;
    if self.count > 10 {
      // panic!("uh oh");
      self.count = 0;
      Err("foobar")?
    }

    Ok(())
  }
}

impl HandleMessage<String> for MyActor {
  async fn handle(&mut self, _handle: &ActorHandle<Self>, message: String) {
    self.count += 1;
    // if self.count > 10 {
    //   panic!("uh oh");
    // }

    info!("received message: {message} total: {}", self.count);
  }
}

impl HandleMessage<Instant> for MyActor {
  async fn handle(&mut self, _handle: &ActorHandle<Self>, message: Instant) {
    info!("got instant");
  }
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt::init();

  // let mgr = MyManager {
  //   token: CancellationToken::new(),
  // };

  // let task = spawn(mgr);
  // let simple_task = spawn_simple(async move {
  //   tokio::time::sleep(Duration::from_secs(5)).await;
  //   println!("I should die now");
  // });

  // task.link_sibling(simple_task);

  // tokio::time::sleep(Duration::from_secs(100)).await;
  // task.kill(StopReason::Abnormal);

  // tokio::time::sleep(Duration::from_secs(10)).await;

  // let queue = Arc::new(Queue::unbounded());

  // let mut task = MyTask;
  // task.on_start(queue.clone()).await;

  // tokio::task::spawn(async move {
  //   loop {
  //     match queue.recv().await {
  //       TaskMessage::User(x) => task.on_message(x).await,
  //       TaskMessage::System(x) => println!("got system message {x:?}"),
  //     }
  //   }
  // });

  // let mut foo = Foo { num: 1 };
  // let fut1 = foo.bar();
  // let fut2 = foo.bar();

  // fut1.await;
  // fut2.await;

  // let handle = spawn_actor(MyActor { count: 0 });
  let handle = ActorPool::new(1, || MyActor { count: 0 }).spawn();
  handle
    .clone()
    .attach_stream_factory(|| IntervalStream::new(interval(Duration::from_secs(1))));

  let queue = Arc::new(DurableQueue::<QueueMessages>::new(
    3,
    QueueSize::Unbounded,
    QueueSize::Bounded(2),
  ));
  INT_EXCHANGE.bind(queue.clone()).await;
  STRING_EXCHANGE.bind(queue.clone()).await;
  // queue.clone().subscribe_to_arc(&INT_EXCHANGE).await;
  // queue.clone().subscribe_to_arc(&STRING_EXCHANGE).await;

  // spawn_worker("worker01", queue.clone());
  // spawn_worker("worker02", queue.clone());

  tokio::task::spawn(async move {
    tokio::time::sleep(Duration::from_secs(5)).await;
    handle.stop(StopReason::Normal);
    // spawn_worker_with_queue("worker03").await;
  });

  let mut v = 0;
  loop {
    if v % 2 == 0 {
      INT_EXCHANGE.send(v).await;
    } else {
      STRING_EXCHANGE.send(format!("string! {v}")).await;
    }

    v += 1;
    tokio::time::sleep(Duration::from_millis(100)).await;
  }

  // println!("{}", queue.recv().await.1);
}

async fn spawn_worker_with_queue(name: &'static str) {
  let queue = Arc::new(Queue::<ExchangeMessage<i32>>::unbounded());
  INT_EXCHANGE.bind(queue.clone()).await;
  tokio::spawn(async move {
    loop {
      let msg = queue.recv().await;
      println!("{name}: {:?}", msg);
    }
  });
}

fn spawn_worker(name: &'static str, queue: Arc<DurableQueue<QueueMessages>>) {
  tokio::spawn(async move {
    loop {
      let msg = queue.clone().recv().await;
      match msg {
        Ok(delivery) => {
          // delivery.ack();
          println!("{name}: {:?}", *delivery);

          if let QueueMessages::Int(x) = &*delivery {
            if *x == 10 && name == "worker01" {
              panic!("fuck");
            }
          }

          delivery.ack();
        }
        Err(x) => println!("err {x:?}"),
      }
    }
  });
}

async fn test_lifetime<'b, 'a: 'b>(exchange: &'b Exchange<'a, i32>, queue: &'a Queue<ExchangeMessage<i32>>) {
  exchange.bind(queue).await;
}
