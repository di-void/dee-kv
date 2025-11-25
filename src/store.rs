use std::collections::HashMap;

#[derive(Clone)]
pub enum Types {
    String(String),
}

pub struct Store {
    // only stores string values for now
    _store: HashMap<String, Types>,
}

// for testing
impl Default for Store {
    fn default() -> Self {
        let test_kv = [("foo", "bar"), ("boo", "baz"), ("dee", "kv")];

        let test_hash = test_kv
            .iter()
            .map(|(k, v)| (k.to_string(), Types::String(v.to_string())))
            .collect::<HashMap<_, _>>();

        Self { _store: test_hash }
    }
}

impl Store {
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

impl From<String> for Types {
    fn from(value: String) -> Self {
        Types::String(value)
    }
}
