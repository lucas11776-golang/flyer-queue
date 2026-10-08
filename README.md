# Flyer Event Emitter (`flyer-event-emitter`)

A blazing-fast, asynchronous event emitter for Rust built on top of `tokio`. It features **Trie-based topic matching**, **single-level wildcards**, **seamless serialization via `serde` and `bincode`**, and **RAII automatic subscription cleanup**.

---

## Features

- **Asynchronous (`tokio`)**: Fully non-blocking event publishing and handling powered by Tokio tasks.
- **Trie-Based Routing**: Fast, segment-by-segment topic routing for high-throughput event dispatching.
- **Wildcard Subscriptions**: Subscribe to flexible topic patterns using `*` (e.g., `users.*.created`).
- **Type Safety**: Native support for strongly typed events using `serde` and `bincode`.
- **Concurrency Control**: Optional semaphore-based limiting (`with_max_in_flight`) to protect downstream services from overload.
- **RAII Subscriptions**: Subscriptions automatically unregister themselves when the `Subscription` guard goes out of scope.

---

## Installation

Add `flyer-event-emitter` to your `Cargo.toml`:

```toml
[dependencies]
flyer-event-emitter = "0.0.1" # Or specify the path / git repository
tokio = { version = "1.53.1", features = ["full"] }
serde = { version = "1.0.229", features=["derive"] }
bytes = "1.12.1"
bincode = "1.3.3"
```

---

## Usage Guide & Examples

### 1. Basic Emission (`Bytes`)
For maximum performance and flexibility, you can emit and subscribe to raw `Bytes`.

```rust
use std::sync::{Arc, LazyLock, RwLock};
use flyer_event_emitter::{EventEmitter, Bytes};

static QUEUE: LazyLock<RwLock<Arc<EventEmitter>>> = LazyLock::new(|| RwLock::new(EventEmitter::new()));

#[tokio::main]
pub async fn main() {
    let _subscription = QUEUE
        .read()
        .unwrap()
        .subscribe(Arc::from("count"), async |payload: Bytes| {
            println!("Received: {}", String::from_utf8_lossy(&payload));
        });

    let queue = QUEUE.read().unwrap();
    let mut count = 0;

    loop {
        queue
            .emit(Arc::from("count"), Bytes::from(format!("{}", count)))
            .await;

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        count += 1;
    }
}
```

---

### 2. Type-Safe Events (`emit_as` & `subscribe_as`)
Easily pass structured data across your application using `serde` serialization.

```rust
use std::sync::{Arc, LazyLock, RwLock};
use flyer_event_emitter::EventEmitter;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Notification {
    pub identify: String,
    pub title: String,
    pub body: String,
}

static QUEUE: LazyLock<RwLock<Arc<EventEmitter>>> = LazyLock::new(|| RwLock::new(EventEmitter::new()));

#[tokio::main]
pub async fn main() {
    let _sub_mail = QUEUE
        .read()
        .unwrap()
        .subscribe_as::<Notification, _>(Arc::from("notification.mail"), async |payload| {
            println!("\nMail Notification: {:?}", payload);
        });

    let _sub_mobile = QUEUE
        .read()
        .unwrap()
        .subscribe_as::<Notification, _>(Arc::from("notification.mobile"), async |payload| {
            println!("\nMobile Notification: {:?}", payload);
        });

    let queue = QUEUE.read().unwrap();
    let mut count = 0;

    loop {
        if count % 2 == 0 {
            queue
                .emit_as(Arc::from("notification.mail"), &Notification {
                    identify: String::from("jeo@doe.com"),
                    title: String::from("New notification"),
                    body: format!("Hello event {}", count + 1),
                })
                .await;
        } else {
            queue
                .emit_as(Arc::from("notification.mobile"), &Notification {
                    identify: String::from("+27745932487"),
                    title: String::new(),
                    body: format!("Hello count {}", count + 1),
                })
                .await;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        count += 1;
    }
}
```

---

### 3. Wildcard Subscriptions (`*`)
Use `*` to match any single segment within a dot-separated topic path.

```rust
use std::sync::{Arc, LazyLock, RwLock};
use flyer_event_emitter::EventEmitter;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct User {
    pub id: u64,
    pub email: String,
}

static QUEUE: LazyLock<RwLock<Arc<EventEmitter>>> = LazyLock::new(|| RwLock::new(EventEmitter::new()));

#[tokio::main]
pub async fn main() {
    // Subscribes to any event matching `users.<anything>.created`
    let _sub = QUEUE
        .read()
        .unwrap()
        .subscribe_as::<User, _>(Arc::from("users.*.created"), async |payload| {
            println!("\nUser Created Event: {:?}", payload);
        });

    let queue = QUEUE.read().unwrap();
    let mut count = 0;

    loop {
        queue
            .emit_as(Arc::from(format!("users.{}.created", count + 1)), &User {
                id: count + 1,
                email: format!("jeo-{}@doe.com", count + 1),
            })
            .await;

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        count += 1;
    }
}
```

---

### 4. Best Practice: Global Event Bus Module Pattern
In real-world applications, wrapping your `EventEmitter` in a clean module API with helper functions provides ergonomic access across your codebase.

```rust
use std::sync::{Arc, LazyLock};
use flyer_event_emitter::{Bytes, EventEmitter, Subscription, AsyncCallback};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::RwLock;

static QUEUE: LazyLock<RwLock<Arc<EventEmitter>>> = LazyLock::new(|| RwLock::new(EventEmitter::new()));

#[derive(Serialize, Deserialize, Debug)]
pub struct Notification {
    pub identifier: String,
    pub title: String,
    pub message: String,
}

pub async fn emit(event: &str, payload: Bytes) {
    QUEUE.read().await.emit(Arc::from(event), payload).await;
}

pub async fn subscribe<C>(event: &str, callback: C) -> Subscription
where
    C: AsyncCallback<Bytes>,
{
    QUEUE.read().await.subscribe(Arc::from(event), callback)
}

pub async fn emit_as<J>(event: &str, payload: &J)
where
    J: Serialize,
{
    QUEUE.read().await.emit_as(Arc::from(event), payload).await;
}

pub async fn subscribe_as<J, C>(event: &str, callback: C) -> Subscription
where
    J: DeserializeOwned + Send + 'static,
    C: AsyncCallback<J>,
{
    QUEUE.read().await.subscribe_as(Arc::from(event), callback)
}

#[tokio::main]
pub async fn main() {
    let _sub_mail = subscribe_as::<Notification, _>("notification.mail", async |payload| {
        println!("\nSending mail message: {:?}", payload);
    });

    let _sub_mobile = subscribe_as::<Notification, _>("notification.mobile", async |payload| {
        println!("\nSending mobile message: {:?}", payload);
    });

    let mut count = 0;

    loop {
        if count % 2 == 0 {
            emit_as("notification.mail", &Notification {
                identifier: String::from("jeo@doe.com"),
                title: String::from("New notification"),
                message: format!("Hello event {}", count + 1),
            })
            .await;
        } else {
            emit_as("notification.mobile", &Notification {
                identifier: String::from("+27745932487"),
                title: String::new(),
                message: format!("Hello count {}", count + 1),
            })
            .await;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        count += 1;
    }
}
```

---

## Advanced Configuration

### Limiting In-Flight Concurrency
If your event handlers perform heavy I/O or CPU operations and you want to prevent system overload, you can restrict concurrent executions using `with_max_in_flight`:

```rust
use flyer_event_emitter::EventEmitter;
use std::sync::Arc;

// Limits concurrent handler task execution to 100
let emitter: Arc<EventEmitter> = EventEmitter::with_max_in_flight(100);
```

---

## How It Works

1. **Trie Storage**: Topics are segmented by dots (`.`) and stored in a hierarchical Trie structure.
2. **Subscription Management**: Subscriptions generate a unique handler ID registered at the corresponding Trie node.
3. **Event Dispatching**: Emitting an event traverses the Trie, matching both exact nodes and single-level wildcards (`*`), then spawns asynchronous Tokio worker tasks.
4. **Thread Safety**: Built using `ArcSwap` and `Arc` for lock-free, ultra-fast concurrent reads, with a fine-grained `Mutex` for subscription updates.

---

## License

Licensed under the [MIT License](LICENSE).
