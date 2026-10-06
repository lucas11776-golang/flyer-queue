use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ahash::RandomState as FastHasher;
use arc_swap::ArcSwap;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::Semaphore;

pub use bytes::Bytes;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

type HandlerFn = Arc<dyn Fn(Bytes) -> BoxFuture<'static, ()> + Send + Sync>;

type HandlerMap = HashMap<u64, HandlerFn, FastHasher>;

static NEXT_SUB_ID: AtomicU64 = AtomicU64::new(1);

pub struct Subscription {
    pub id: u64,
    pub event: Arc<str>,
    pub queue: Arc<EventEmitter>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.queue.unsubscribe(&self.event, self.id);
    }
}

pub trait AsyncCallback<Args>: Send + Sync + 'static {
    fn call(&self, args: Args) -> BoxFuture<'static, ()>;
}

impl<F, Fut, Arg> AsyncCallback<Arg> for F
where
    F: Fn(Arg) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
    Arg: 'static,
{
    fn call(&self, args: Arg) -> BoxFuture<'static, ()> {
        Box::pin(self(args))
    }
}

#[derive(Clone)]
pub struct TrieNode {
    pub handlers: Arc<HandlerMap>,
    pub json_capacity_hint: Arc<AtomicUsize>,
    pub children: HashMap<String, Arc<TrieNode>, FastHasher>,
    pub wildcard_single: Option<Arc<TrieNode>>,
}

impl Default for TrieNode {
    fn default() -> Self {
        Self {
            handlers: Arc::new(HashMap::with_hasher(FastHasher::default())),
            json_capacity_hint: Arc::new(AtomicUsize::new(256)),
            children: HashMap::with_hasher(FastHasher::default()),
            wildcard_single: None,
        }
    }
}

impl TrieNode {
    pub fn collect_matching_handlers<F>(&self, event: &str, callback: &mut F)
    where
        F: FnMut(&HandlerFn),
    {
        if event.is_empty() {
            for handler in self.handlers.values() {
                callback(handler);
            }
            return;
        }

        let (head, rest) = match event.find('.') {
            Some(idx) => (&event[..idx], &event[idx + 1..]),
            None => (event, ""),
        };

        if let Some(child) = self.children.get(head) {
            child.collect_matching_handlers(rest, callback);
        }

        if let Some(ref single) = self.wildcard_single {
            single.collect_matching_handlers(rest, callback);
        }
    }

    pub fn find_exact_node(&self, event: &str) -> Option<Arc<TrieNode>> {
        if event.is_empty() {
            return Some(Arc::new(self.clone()));
        }
        let (head, rest) = match event.find('.') {
            Some(idx) => (&event[..idx], &event[idx + 1..]),
            None => (event, ""),
        };

        if head == "*" {
            self.wildcard_single.as_ref()?.find_exact_node(rest)
        } else {
            self.children.get(head)?.find_exact_node(rest)
        }
    }
}

pub struct EventEmitter {
    root: ArcSwap<TrieNode>,
    write_lock: Mutex<()>,
    limiter: Option<Arc<Semaphore>>,
}

impl EventEmitter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            root: ArcSwap::from_pointee(TrieNode::default()),
            write_lock: Mutex::new(()),
            limiter: None,
        })
    }

    pub fn with_max_in_flight(max_concurrent_tasks: usize) -> Arc<Self> {
        Arc::new(Self {
            root: ArcSwap::from_pointee(TrieNode::default()),
            write_lock: Mutex::new(()),
            limiter: Some(Arc::new(Semaphore::new(max_concurrent_tasks))),
        })
    }

    fn do_insert_handler(node: &TrieNode, event: &str, id: u64, handler: HandlerFn) -> TrieNode {
        let mut new_node = node.clone();

        if event.is_empty() {
            let mut new_handlers = (*new_node.handlers).clone();
            new_handlers.insert(id, handler);
            new_node.handlers = Arc::new(new_handlers);
            return new_node;
        }

        let (head, rest) = match event.find('.') {
            Some(idx) => (&event[..idx], &event[idx + 1..]),
            None => (event, ""),
        };

        if head == "*" {
            let target = new_node
                .wildcard_single
                .as_deref()
                .cloned()
                .unwrap_or_default();
            let updated = Self::do_insert_handler(&target, rest, id, handler);
            new_node.wildcard_single = Some(Arc::new(updated));
        } else {
            let target = new_node
                .children
                .get(head)
                .map(|c| (**c).clone())
                .unwrap_or_default();
            let updated = Self::do_insert_handler(&target, rest, id, handler);
            new_node.children.insert(head.to_string(), Arc::new(updated));
        }

        new_node
    }

    fn insert_handler(&self, event: &str, handler: HandlerFn) -> u64 {
        let id = NEXT_SUB_ID.fetch_add(1, Ordering::Relaxed);
        let _guard = self.write_lock.lock().unwrap();

        let current_root = self.root.load();


        let updated_root = Self::do_insert_handler(&current_root, event, id, handler);

        self.root.store(Arc::new(updated_root));

        id
    }

    pub async fn emit(&self, event: Arc<str>, payload: Bytes) {
        let root = self.root.load();
        let mut futures = Vec::new();

        root.collect_matching_handlers(&event, &mut |handler| {
            futures.push(handler(payload.clone()));
        });

        if futures.is_empty() {
            return;
        }

        let limiter = self.limiter.clone();

        tokio::spawn(async move {
            let _permit = if let Some(ref lim) = limiter {
                lim.acquire().await.ok()
            } else {
                None
            };

            for fut in futures {
                fut.await;
            }
        });
    }

    pub fn subscribe<C>(self: &Arc<Self>, event: Arc<str>, callback: C) -> Subscription
    where
        C: AsyncCallback<Bytes>,
    {
        let cb = Arc::new(callback);

        let handler: HandlerFn = Arc::new(move |payload: Bytes| {
            let cb = Arc::clone(&cb);

            Box::pin(async move {
                cb.call(payload).await;
            })
        });

        Subscription {
            id: self.insert_handler(&event, handler),
            event: event,
            queue: Arc::clone(self),
        }
    }

    pub async fn emit_as<J>(&self, event: Arc<str>, payload: &J)
    where
        J: Serialize,
    {
        if let Ok(bytes_vec) = bincode::serialize(payload) {
            self.emit(event, Bytes::from(bytes_vec)).await;
        }
    }

    pub fn subscribe_as<J, C>(self: &Arc<Self>, event: Arc<str>, callback: C) -> Subscription
    where
        J: DeserializeOwned + Send + 'static,
        C: AsyncCallback<J>,
    {
        let cb = Arc::new(callback);

        let handler: HandlerFn = Arc::new(move |payload: Bytes| {
            let cb = Arc::clone(&cb);

            Box::pin(async move {
                if let Ok(data) = bincode::deserialize::<J>(&payload) {
                    cb.call(data).await;
                }
            })
        });

        Subscription {
            id: self.insert_handler(&event, handler),
            event: event,
            queue: Arc::clone(self),
        }
    }

    fn do_remove_subscription(node: &TrieNode, event: &str, id: u64) -> (TrieNode, bool) {
        let mut new_node = node.clone();

        if event.is_empty() {
            let mut new_handlers = (*new_node.handlers).clone();
            new_handlers.remove(&id);
            new_node.handlers = Arc::new(new_handlers);

            let empty = new_node.handlers.is_empty()
                && new_node.children.is_empty()
                && new_node.wildcard_single.is_none();

            return (new_node, empty);
        }

        let (head, rest) = match event.find('.') {
            Some(idx) => (&event[..idx], &event[idx + 1..]),
            None => (event, ""),
        };

        if head == "*" {
            if let Some(ref target) = new_node.wildcard_single {
                let (updated, empty) = Self::do_remove_subscription(target, rest, id);
                if empty {
                    new_node.wildcard_single = None;
                } else {
                    new_node.wildcard_single = Some(Arc::new(updated));
                }
            }
        } else if let Some(target) = new_node.children.get(head) {
            let (updated, empty) = Self::do_remove_subscription(target, rest, id);
            if empty {
                new_node.children.remove(head);
            } else {
                new_node.children.insert(head.to_string(), Arc::new(updated));
            }
        }

        let empty = new_node.handlers.is_empty()
            && new_node.children.is_empty()
            && new_node.wildcard_single.is_none();

        (new_node, empty)
    }

    fn unsubscribe(&self, event: &str, id: u64) {
        let _guard = self.write_lock.lock().unwrap();
        let current_root = self.root.load();

        let (updated_root, _) = Self::do_remove_subscription(&current_root, event, id);

        self.root.store(Arc::new(updated_root));
    }
}
