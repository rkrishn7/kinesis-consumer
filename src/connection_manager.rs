use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub trait ConnectionManager {
    fn increment_connections(&self, app_name: &str, streams: &Vec<String>);
    fn decrement_connections(
        &self,
        app_name: &str,
        streams: &Vec<String>,
    ) -> Result<(), ()>;
    fn get_connection_count(&self, app_name: &str, stream: &str) -> usize;
}

#[derive(PartialEq, Eq, Hash, Clone)]
struct ConnectionKey {
    app_name: String,
    stream_name: String,
}

impl ConnectionKey {
    pub fn new(app_name: String, stream_name: String) -> Self {
        Self {
            app_name,
            stream_name,
        }
    }
}

#[derive(Clone)]
pub struct MemoryConnectionManager {
    inner: Arc<Mutex<HashMap<ConnectionKey, usize>>>,
}

impl MemoryConnectionManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl ConnectionManager for MemoryConnectionManager {
    fn increment_connections(&self, app_name: &str, streams: &Vec<String>) {
        for stream in streams.iter() {
            let key =
                ConnectionKey::new(app_name.to_string(), stream.to_owned());
            match self.inner.lock().unwrap().entry(key) {
                Entry::Occupied(mut e) => {
                    *e.get_mut() += 1;
                }
                Entry::Vacant(e) => {
                    e.insert(1);
                }
            }
        }
    }

    fn decrement_connections(
        &self,
        app_name: &str,
        streams: &Vec<String>,
    ) -> Result<(), ()> {
        for stream in streams.iter() {
            let key =
                ConnectionKey::new(app_name.to_string(), stream.to_owned());
            match self.inner.lock().unwrap().entry(key) {
                Entry::Occupied(mut e) => {
                    *e.get_mut() -= 1;
                }
                Entry::Vacant(_) => return Err(()),
            }
        }

        Ok(())
    }

    fn get_connection_count(&self, app_name: &str, stream: &str) -> usize {
        let key = ConnectionKey::new(app_name.to_string(), stream.to_string());

        match self.inner.lock().unwrap().get(&key) {
            Some(count) => *count,
            None => 0,
        }
    }
}
