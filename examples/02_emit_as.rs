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
            println!("\r\n{:?}", payload);
        });

    let _sub_mobile = QUEUE
        .read()
        .unwrap()
        .subscribe_as::<Notification, _>(Arc::from("notification.mobile"), async |payload| {
            println!("\r\n{:?}", payload);
        });

    let queue = QUEUE
        .read()
        .unwrap();

    let mut count = 0;

    loop {
        if count % 2 == 0 {
            queue
                .emit_as(Arc::from("notification.mail"), &Notification {
                    identify: String::from("jeo@doe.com"),
                    title: String::from("New notification"),
                    body: format!("Hello event {}", count+1)
                })
                .await;
        } else {
            queue
                .emit_as(Arc::from("notification.mobile"), &Notification {
                    identify: String::from("+27745932487"),
                    title: String::new(),
                    body: format!("Hello count {}", count+1)
                })
                .await;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        count += 1;
    }
}

