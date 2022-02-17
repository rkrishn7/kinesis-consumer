use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryConnectionTableKey {
    app_name: String,
    stream_name: String,
}

#[derive(Clone)]
pub struct MemoryConnectionTable(
    Arc<Mutex<BTreeMap<MemoryConnectionTableKey, BTreeSet<SocketAddr>>>>,
);

impl MemoryConnectionTable {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(BTreeMap::new())))
    }
}

#[async_trait]
pub trait ConnectionTable {
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

#[async_trait]
impl ConnectionTable for MemoryConnectionTable {
    async fn register(
        &self,
        app_name: String,
        streams: Vec<String>,
        socket_addr: SocketAddr,
    ) {
        let map = &mut *self.0.lock().unwrap();
        for stream in streams {
            let key = MemoryConnectionTableKey {
                app_name: app_name.clone(),
                stream_name: stream,
            };

            map.entry(key).or_default().insert(socket_addr);
        }
    }

    async fn get_count(&self, app_name: String, stream_name: String) -> usize {
        let map = &*self.0.lock().unwrap();

        let key = MemoryConnectionTableKey {
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

        let key = MemoryConnectionTableKey {
            app_name,
            stream_name,
        };

        if let Some(v) = map.get(&key) {
            return v.contains(&socket_addr);
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_registers_record_processor() {
        let app_name: String = "app_test".into();
        let streams =
            vec!["stream1".into(), "stream2".into(), "stream3".into()];

        let register = MemoryConnectionTable::new();

        register
            .register(
                app_name.clone(),
                streams,
                "127.0.0.1:8080".parse().unwrap(),
            )
            .await;

        assert_eq!(
            register.get_count(app_name.clone(), "stream1".into()).await,
            1
        );

        assert_eq!(
            register.get_count(app_name.clone(), "stream2".into()).await,
            1
        );

        assert_eq!(
            register.get_count(app_name.clone(), "stream3".into()).await,
            1
        );
    }

    #[tokio::test]
    async fn test_removes_record_processor() {
        let app_name: String = "app_test".into();
        let streams =
            vec!["stream1".into(), "stream2".into(), "stream3".into()];

        let register = MemoryConnectionTable::new();

        register
            .register(
                app_name.clone(),
                streams,
                "127.0.0.1:8080".parse().unwrap(),
            )
            .await;

        register.remove("127.0.0.1:8080".parse().unwrap()).await;

        assert_eq!(
            register.get_count(app_name.clone(), "stream1".into()).await,
            0
        );

        assert_eq!(
            register.get_count(app_name.clone(), "stream2".into()).await,
            0
        );

        assert_eq!(
            register.get_count(app_name.clone(), "stream3".into()).await,
            0
        );
    }

    #[tokio::test]
    async fn test_checks_if_record_processor_is_registered() {
        let app_name: String = "app_test".into();
        let streams =
            vec!["stream1".into(), "stream2".into(), "stream3".into()];
        let socket_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        let register = MemoryConnectionTable::new();

        register
            .register(app_name.clone(), streams, socket_addr)
            .await;

        assert!(
            register
                .is_registered(app_name.clone(), "stream1".into(), socket_addr)
                .await
        );

        assert!(
            register
                .is_registered(app_name.clone(), "stream2".into(), socket_addr)
                .await
        );

        assert!(
            register
                .is_registered(app_name.clone(), "stream3".into(), socket_addr)
                .await
        );

        assert!(
            !register
                .is_registered(app_name.clone(), "stream4".into(), socket_addr)
                .await
        );
    }
}
