use std::collections::HashMap;

#[derive(Clone)]
pub enum Types {
    String(String),
}

pub struct Store {
    // only stores string values for now
    _store: HashMap<String, Types>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            _store: HashMap::new(),
        }
    }

    pub fn get(&self, k: &str) -> Option<Types> {
        if let Some(val) = self._store.get(k) {
            Some(val.to_owned())
        } else {
            None
        }
    }

    pub fn set(&mut self, kv: (&str, Types)) {
        self._store.insert(kv.0.to_string(), kv.1);
    }

    pub fn delete(&mut self, k: &str) {
        self._store.remove(k);
    }
}
