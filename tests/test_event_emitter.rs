#[cfg(test)]
mod test_queue {
    use std::sync::Arc;

    use bytes::Bytes;
    use flyer_event_emitter::EventEmitter;
    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc;
    
    #[tokio::test]
    pub async fn test_emit_event() {
        let queue = EventEmitter::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let _subscription = queue.subscribe(Arc::from("event"), move |payload: Bytes| {
            let tx = tx.clone();
            async move {
                tx
                    .send( String::from_utf8_lossy(&payload).to_string())
                    .expect("Receiver dropped");
            }
        });

        queue.emit(Arc::from("event"), Bytes::from("Emitting Event Message")).await;

        assert_eq!(rx.recv().await.unwrap(), "Emitting Event Message");
    }    

    #[tokio::test]
    pub async fn test_emit_as_event() {
        #[derive(Deserialize, Debug, Serialize, PartialEq, Clone)]
        pub struct Notification {
            pub email: String,
            pub title: String,
            pub body: String,
        }

        let queue = EventEmitter::with_max_in_flight(1_0000);
        let (tx, mut rx) = mpsc::unbounded_channel();

        let _subscription = queue.subscribe_as::<Notification, _>("notification.email".into(), move |payload| {
            let tx = tx.clone();
            async move {
                tx
                    .send(payload)
                    .expect("Receiver dropped");
            }
        });

        let payload = Notification {
            email: String::from("jeo@doe.com"),
            title: String::from("New Account"),
            body: String::from("New account has be registered")
        };

        queue.emit_as(Arc::from("notification.email"), &payload).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert_eq!(rx.recv().await.unwrap(), payload);
    }

    #[tokio::test]
    pub async fn test_subscribe_wildcard_event() {
        #[derive(Deserialize, Debug, Serialize, PartialEq, Clone)]
        pub struct User {
            pub id: u64,
            pub email: String,
        }

        let queue = EventEmitter::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let _subscription = queue.subscribe_as::<User, _>("users.*.created".into(), move |payload| {
            let tx = tx.clone();
            async move {
                tx
                    .send(payload)
                    .expect("Receiver dropped");
            }
        });

        let users: Vec<User> = vec![
            User { id: 1, email: String::from("jeo@doe.com") },
            User { id: 2, email: String::from("jane@doe.com") },
            User { id: 3, email: String::from("james@doe.com") },
        ];

        for user in &users {
            queue.emit_as(Arc::from(format!("users.{}.created", user.id)), &user).await;
        }
       
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert_eq!(rx.recv().await.unwrap(), users[0]);
        assert_eq!(rx.recv().await.unwrap(), users[1]);
        assert_eq!(rx.recv().await.unwrap(), users[2]);
    }
}