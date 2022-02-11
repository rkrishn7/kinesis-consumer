use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};

#[async_trait]
pub trait RecordProcessorRegister {
    async fn register(
        &self,
        app_name: String,
        streams: Vec<String>,
        socket_addr: SocketAddr,
    );
    async fn is_registered(
        &self,
        app_name: String,
        stream_name: String,
        socket_addr: SocketAddr,
    ) -> bool;
    async fn remove(&self, socket_addr: SocketAddr);
    async fn get_count(&self, app_name: String, stream_name: String) -> usize;
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Key {
    app_name: String,
    stream_name: String,
}

#[derive(Clone)]
pub struct MemoryRecordProcessorRegister(
    Arc<Mutex<BTreeMap<Key, BTreeSet<SocketAddr>>>>,
);

impl MemoryRecordProcessorRegister {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(BTreeMap::new())))
    }
}

#[async_trait]
impl RecordProcessorRegister for MemoryRecordProcessorRegister {
    async fn register(
        &self,
        app_name: String,
        streams: Vec<String>,
        socket_addr: SocketAddr,
    ) {
        let map = &mut *self.0.lock().unwrap();
        for stream in streams {
            let key = Key {
                app_name: app_name.clone(),
                stream_name: stream,
            };

            map.entry(key).or_default().insert(socket_addr);
        }
    }

    async fn get_count(&self, app_name: String, stream_name: String) -> usize {
        let map = &*self.0.lock().unwrap();

        let key = Key {
            app_name,
            stream_name,
        };

        map.get(&key).map(|v| v.len()).unwrap_or(0)
    }

    async fn remove(&self, socket_addr: SocketAddr) {
        let map = &mut *self.0.lock().unwrap();

        for val in map.values_mut() {
            val.remove(&socket_addr);
        }
    }

    async fn is_registered(
        &self,
        app_name: String,
        stream_name: String,
        socket_addr: SocketAddr,
    ) -> bool {
        let map = &*self.0.lock().unwrap();

        let key = Key {
            app_name,
            stream_name,
        };

        if let Some(v) = map.get(&key) {
            return v.contains(&socket_addr);
        }

        false
    }
}
