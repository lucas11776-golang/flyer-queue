



#[cfg(test)]
mod test_benchmark_queue {
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;
    use flyer_event_emitter::EventEmitter;
    use tokio::{sync::mpsc, time::timeout};

    #[tokio::test]
    pub async fn test_must_take_minimum_of_million_events_in_2_seconds() {
        let queue = EventEmitter::new();
        let (tx, mut rx) = mpsc::unbounded_channel::<()>();

        let _subscription = queue.subscribe(Arc::from("stats"), move |_payload: Bytes| {
            let tx = tx.clone();
            async move {
                tx.send(()).expect("Receiver dropped");
            }
        });

        // Spawn emitter concurrently (do NOT .await here)
        let emitter_handle = tokio::spawn(async move {
            for _ in 0..1_000_000 {
                queue.emit(Arc::from("stats"), Bytes::new()).await;
            }
        });

        let receive_result = timeout(Duration::from_millis(2000), async {
            let mut count = 0;
            while count < 1_000_000 {
                rx.recv().await.expect("Channel closed prematurely");
                count += 1;
            }
            count
        })
        .await;

        assert!(receive_result.is_ok(), "Failed to receive 1 million events within 2 seconds");
        assert_eq!(receive_result.unwrap(), 1_000_000);
        
        emitter_handle.await.unwrap();
    }
}
