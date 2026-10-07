#[cfg(test)]
mod test_benchmark_queue {
    use std::{
        sync::{Arc, atomic::{AtomicU64, Ordering}},
        time::Duration
    };

    use bytes::Bytes;
    use flyer_event_emitter::EventEmitter;
    use tokio::{sync::mpsc, time::{sleep, timeout}};

    #[tokio::test]
    pub async fn test_must_take_minimum_of_million_events_in_5_seconds() {
        let queue = EventEmitter::new();
        let (tx, mut rx) = mpsc::unbounded_channel::<()>();

        let _subscription = queue.subscribe(Arc::from("stats"), move |_payload: Bytes| {
            let tx = tx.clone();
            async move {
                tx.send(()).expect("Receiver dropped");
            }
        });

        let emitter_handle = tokio::spawn(async move {
            for _ in 0..1_000_000 {
                queue.emit(Arc::from("stats"), Bytes::new()).await;
            }
        });

        let receive_result = timeout(Duration::from_secs(5), async {
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

    #[tokio::test]
    pub async fn test_10k_subscribes_must_take_1k_events_in_5_seconds() {
        let queue = EventEmitter::new();
        let received_count = Arc::new(AtomicU64::new(0));

        const SUBSCRIBERS: usize = 10_000;
        const EVENTS: u64 = 1_000;
        const TOTAL_EXPECTED: u64 = (SUBSCRIBERS as u64) * EVENTS;

        let mut _subscriptions = Vec::with_capacity(SUBSCRIBERS);
        
        let topic: Arc<str> = Arc::from("stats");

        for _ in 0..SUBSCRIBERS {
            let count = Arc::clone(&received_count);
            let queue_clone = Arc::clone(&queue);
            let topic_clone = Arc::clone(&topic);

            _subscriptions.push(
                queue_clone.subscribe(topic_clone, move |_payload: Bytes| {
                    let count = Arc::clone(&count);
                    async move {
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                })
            );
        }

        let emitter_handle = tokio::spawn(async move {
            let payload = Bytes::new();
            for _ in 0..EVENTS {
                queue.emit(Arc::clone(&topic), payload.clone()).await;
            }
        });

        let receive_result = timeout(Duration::from_secs(5), async {
            while received_count.load(Ordering::Relaxed) < TOTAL_EXPECTED {
                sleep(Duration::from_nanos(100)).await;
            }
            received_count.load(Ordering::Relaxed)
        })
        .await;

        assert!(
            receive_result.is_ok(),
            "Processed {}/{} deliveries in 5 seconds",
            received_count.load(Ordering::Relaxed),
            TOTAL_EXPECTED
        );
        assert_eq!(receive_result.unwrap(), TOTAL_EXPECTED);

        emitter_handle.await.unwrap();
    }
}
