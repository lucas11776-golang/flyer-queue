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
    let _sub = QUEUE
        .read()
        .unwrap()
        .subscribe_as::<User, _>(Arc::from("users.*.created"), async |payload| {
            println!("\r\n{:?}", payload);
        });

    let queue = QUEUE
        .read()
        .unwrap();

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

