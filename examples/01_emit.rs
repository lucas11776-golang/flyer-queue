use std::sync::{Arc, LazyLock, RwLock};

use flyer_event_emitter::{EventEmitter, Bytes};

static QUEUE: LazyLock<RwLock<Arc<EventEmitter>>> = LazyLock::new(|| RwLock::new(EventEmitter::new()));

#[tokio::main]
pub async fn main() {
    let _subscription = QUEUE
        .read()
        .unwrap()
        .subscribe(Arc::from("count"), async |payload: Bytes| {
            println!("{}", String::from_utf8_lossy(&payload));
        });

    let queue = QUEUE
        .read()
        .unwrap();

    let mut count = 0;

    loop {
        queue
            .emit(Arc::from("count"), Bytes::from(format!("{}", count)))
            .await;

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        count += 1;
    }
}

