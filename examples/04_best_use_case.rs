use std::sync::{Arc, LazyLock};

use flyer_event_emitter::{Bytes, EventEmitter, Subscription, AsyncCallback};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::RwLock;

///
/// 
/// 

static QUEUE: LazyLock<RwLock<Arc<EventEmitter>>> = LazyLock::new(|| RwLock::new(EventEmitter::new()));

#[derive(Serialize, Deserialize, Debug)]
pub struct Notification {
    pub identifier: String,
    pub title: String,
    pub message: String,
}


pub async fn emit(event: &str, payload: Bytes) {
    QUEUE
        .read()
        .await
        .emit(Arc::from(event), payload)
        .await
}

pub async fn subscribe<C>(event: &str, callback: C) -> Subscription
where
    C: AsyncCallback<Bytes>,
{
    QUEUE
        .read()
        .await
        .subscribe(Arc::from(event), callback)
}

pub async fn emit_as<J>(event: &str, payload: &J)
where
    J: Serialize
{
    QUEUE
        .read()
        .await
        .emit_as(Arc::from(event), payload)
        .await
}

pub async fn subscribe_as<J, C>(event: &str, callback: C) -> Subscription
where
    J: DeserializeOwned + Send + 'static,
    C: AsyncCallback<J>
{
    QUEUE
        .read()
        .await
        .subscribe_as(Arc::from(event), callback)
}

///
/// 
/// 

#[tokio::main]
pub async fn main() {
    let _sub_mail = subscribe_as::<Notification, _>("notification.mail", async |payload| {
        println!("\r\n Sending mail message: {:?}", payload);
    });

    let _sub_mobile = subscribe_as::<Notification, _>("notification.mobile", async |payload| {
        println!("\r\n Sending mobile message: {:?}", payload);
    });

    let mut count = 0;

    loop {
        if count % 2 == 0 {
            emit_as("notification.mail", &Notification {
                    identifier: String::from("jeo@doe.com"),
                    title: String::from("New notification"),
                    message: format!("Hello event {}", count+1)
                })
                .await;
        } else {
            emit_as("notification.mobile", &Notification {
                identifier: String::from("+27745932487"),
                title: String::new(),
                message: format!("Hello count {}", count+1)
            })
            .await;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        count += 1;
    }
}