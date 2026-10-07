# Flyer Event Emitter

A high-performance, asynchronous event emitter for Rust, featuring Trie-based topic matching, single-level wildcards, and seamless serialization.

## Features

- **Asynchronous**: Built on top of `tokio` for efficient async/await support.
- **Trie-based Routing**: Efficient topic matching for high-performance event dispatching.
- **Wildcard Support**: Subscribe to patterns using `*` (e.g., `orders.*.created`).
- **Type Safety**: Built-in support for serializing and deserializing types using `serde` and `bincode`.
- **Concurrency Control**: Optional semaphore-based limiting for in-flight event processing.
- **RAII Subscriptions**: Subscriptions are automatically removed when they go out of scope.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
flyer-event-emitter = "0.0.0" # Replace with actual version
tokio = { version = "1", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
bytes = "1"
```

## Quick Start

### Basic Usage

Use raw `Bytes` for maximum flexibility and performance.

```rust
use std::sync::Arc;
use flyer_event_emitter::{EventEmitter, Bytes};

#[tokio::main]
async fn main() {
    let emitter = EventEmitter::new();

    // Subscribe to a topic
    let _sub = emitter.subscribe(Arc::from("log.info"), |payload: Bytes| async move {
        println!("Received: {}", String::from_utf8_lossy(&payload));
    });

    // Emit an event
    emitter.emit(Arc::from("log.info"), Bytes::from("Hello, World!")).await;
}
```

### Typed Events

Easily emit and subscribe to complex data structures using `serde`.

```rust
use std::sync::Arc;
use flyer_event_emitter::EventEmitter;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct UserAction {
    user_id: u32,
    action: String,
}

#[tokio::main]
async fn main() {
    let emitter = EventEmitter::new();

    let _sub = emitter.subscribe_as::<UserAction, _>(Arc::from("user.activity"), |payload| async move {
        println!("User {} performed: {}", payload.user_id, payload.action);
    });

    emitter.emit_as(Arc::from("user.activity"), &UserAction {
        user_id: 42,
        action: "login".to_string(),
    }).await;
}
```

### Wildcard Subscriptions

Use `*` to match any single segment in a topic path.

```rust
use std::sync::Arc;
use flyer_event_emitter::EventEmitter;

#[tokio::main]
async fn main() {
    let emitter = EventEmitter::new();

    // Matches 'sensor.1.temp', 'sensor.room_a.temp', etc.
    let _sub = emitter.subscribe(Arc::from("sensor.*.temp"), |payload| async move {
        println!("Temperature update received");
    });

    emitter.emit(Arc::from("sensor.kitchen.temp"), "22.5".into()).await;
}
```

## Detailed Example: Multi-Service Architecture

This example demonstrates how `flyer-event-emitter` can be used to coordinate multiple services using wildcards and typed events.

```rust
use std::sync::Arc;
use flyer_event_emitter::EventEmitter;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct Order {
    id: u64,
    item: String,
    status: String,
}

#[tokio::main]
async fn main() {
    // 1. Initialize the shared event emitter (usually stored in a global state or Arc)
    let emitter = EventEmitter::new();

    // 2. Monitoring Service: Subscribes to ALL order events using a wildcard
    // Matches 'orders.created', 'orders.updated', 'orders.deleted', etc.
    let _monitor = emitter.subscribe_as::<Order, _>(Arc::from("orders.*"), |order| async move {
        println!("[Monitor] Order {} status changed to: {}", order.id, order.status);
    });

    // 3. Analytics Service: Specifically interested in 'created' events
    let _analytics = emitter.subscribe_as::<Order, _>(Arc::from("orders.created"), |order| async move {
        println!("[Analytics] New order placed for: {}", order.item);
    });

    // 4. Emit events
    println!("--- Emitting orders.created ---");
    emitter.emit_as(Arc::from("orders.created"), &Order {
        id: 101,
        item: "Laptop".to_string(),
        status: "pending".to_string(),
    }).await;

    // Small delay to allow async tasks to process (in real apps, the emitter handles this)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("\n--- Emitting orders.updated ---");
    emitter.emit_as(Arc::from("orders.updated"), &Order {
        id: 101,
        item: "Laptop".to_string(),
        status: "shipped".to_string(),
    }).await;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}
```

## Advanced Configuration

### Limiting Concurrency

If you are processing high volumes of events and want to avoid overwhelming your system, you can limit the number of concurrent tasks:

```rust
let emitter = EventEmitter::with_max_in_flight(100); // Only 100 concurrent handlers
```

## How it Works

1. **Trie Storage**: Topics are stored in a Trie structure. Each segment of a dot-separated topic (e.g., `a.b.c`) is a node in the tree.
2. **Subscription Management**: When you `subscribe`, a unique handler ID is generated and stored in the corresponding Trie node.
3. **Event Dispatching**: When you `emit`, the emitter traverses the Trie to find all matching handlers (including those matching wildcard nodes) and spawns them as asynchronous tasks.
4. **Memory Safety**: The Trie uses `ArcSwap` and `Arc` to ensure thread-safe, lock-free reads, while `Mutex` protects updates (subscriptions/unsubscriptions).

## License

MIT
