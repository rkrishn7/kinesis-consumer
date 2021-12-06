use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
pub trait ConnectionManager {
    fn add_connection_for_streams(
        &self,
        streams: &Vec<&str>,
        connection: Option<SocketAddr>,
    );
    fn remove_connection_for_streams(
        &self,
        streams: &Vec<&str>,
        connection: Option<SocketAddr>,
    ) -> Result<Option<SocketAddr>, ()>;
    fn get_connections_for_streams(
        &self,
        streams: &Vec<&str>,
    ) -> Vec<Option<SocketAddr>>;
    fn get_connection_count_for_streams(&self, streams: &Vec<&str>) -> usize;
}

#[derive(Clone)]
pub struct MemoryConnectionManager {
    inner: Arc<Mutex<HashMap<String, Vec<Option<SocketAddr>>>>>,
}

impl MemoryConnectionManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl ConnectionManager for MemoryConnectionManager {
    fn add_connection_for_streams(
        &self,
        streams: &Vec<&str>,
        connection: Option<SocketAddr>,
    ) {
        let mut map = self.inner.lock().unwrap();
        for stream in streams.iter() {
            match map.entry(stream.to_string()) {
                Entry::Occupied(mut e) => {
                    e.get_mut().push(connection);
                }
                Entry::Vacant(e) => {
                    e.insert(vec![connection]);
                }
            }
        }
    }

    fn remove_connection_for_streams(
        &self,
        streams: &Vec<&str>,
        connection: Option<SocketAddr>,
    ) -> Result<Option<SocketAddr>, ()> {
        let mut map = self.inner.lock().unwrap();

        for stream in streams.iter() {
            match map.entry(stream.to_string()) {
                Entry::Occupied(mut e) => {
                    match e.get().iter().position(|sa| *sa == connection) {
                        Some(idx) => {
                            let conn = e.get_mut().remove(idx);
                            return Ok(conn);
                        }
                        None => {
                            return Err(());
                        }
                    }
                }
                Entry::Vacant(_) => {
                    return Err(());
                }
            }
        }

        Err(())
    }

    fn get_connections_for_streams(
        &self,
        streams: &Vec<&str>,
    ) -> Vec<Option<SocketAddr>> {
        let mut map = self.inner.lock().unwrap();

        let mut connections = Vec::new();

        for stream in streams.iter() {
            match map.entry(stream.to_string()) {
                Entry::Occupied(e) => {
                    connections.extend(e.get().clone());
                }
                Entry::Vacant(_) => (),
            }
        }

        connections
    }

    fn get_connection_count_for_streams(&self, streams: &Vec<&str>) -> usize {
        self.get_connections_for_streams(streams).len()
    }
}
