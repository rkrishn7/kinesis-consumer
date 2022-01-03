use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::{collections::BTreeMap, net::SocketAddr};

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

#[derive(Clone)]
pub struct MemoryRecordProcessorRegister(
    Arc<Mutex<BTreeMap<(String, String), Vec<SocketAddr>>>>,
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
            map.entry((app_name.clone(), stream))
                .or_default()
                .push(socket_addr);
        }
    }

    async fn get_count(&self, app_name: String, stream_name: String) -> usize {
        let map = &*self.0.lock().unwrap();

        map.get(&(app_name, stream_name))
            .map(|v| v.len())
            .unwrap_or(0)
    }

    async fn remove(&self, socket_addr: SocketAddr) {
        let map = &mut *self.0.lock().unwrap();

        for val in map.values_mut() {
            if let Some(idx) = val.iter().position(|x| *x == socket_addr) {
                val.remove(idx);
            }
        }
    }

    async fn is_registered(
        &self,
        app_name: String,
        stream_name: String,
        socket_addr: SocketAddr,
    ) -> bool {
        let map = &*self.0.lock().unwrap();

        if let Some(v) = map.get(&(app_name, stream_name)) {
            return v.contains(&socket_addr);
        }

        false
    }
}
