use crate::{DATA_DIR, log::load_store};
use anyhow::Context;
use std::collections::HashMap;
use std::path::Path;

#[derive(Clone, Debug)]
pub enum Types {
    String(String),
}

pub struct Store {
    _store: HashMap<String, Types>,
}

impl Default for Store {
    fn default() -> Self {
        Self {
            _store: load_store(Path::new(DATA_DIR))
                .with_context(|| format!("Error occurred while loading store"))
                .unwrap(),
        }
    }
}

impl Store {
    pub fn get(&self, k: &str) -> Option<Types> {
        self._store.get(k).map(|v| v.to_owned())
    }

    pub fn set(&mut self, kv: (&str, Types)) {
        self._store.insert(kv.0.to_string(), kv.1);
    }

    pub fn delete(&mut self, k: &str) -> Option<Types> {
        self._store.remove(k)
    }
}

impl From<String> for Types {
    fn from(value: String) -> Self {
        Types::String(value)
    }
}

impl From<Types> for String {
    fn from(value: Types) -> Self {
        match value {
            Types::String(s) => s,
        }
    }
}
