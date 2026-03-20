use crate::{LogTerm, serde::LogEntry};

// log cache
pub struct LogCache {
    buf: Vec<LogEntry>,
    size: usize,
    _start_log_offset: u16,
}

impl LogCache {
    pub fn from_last_logs(logs: Vec<(LogEntry, usize)>) -> Self {
        let mut size = 0;
        let buf = logs
            .into_iter()
            .map(|item| {
                size += item.1;
                item.0
            })
            .collect::<Vec<LogEntry>>();

        Self {
            buf,
            size,
            _start_log_offset: 0,
        }
    }

    pub fn push(&mut self, entry: LogEntry, size: usize) {
        self.buf.push(entry);
        self.size += size
    }

    async fn _rotate(&mut self) {
        todo!("rotate")
    }

    pub async fn get_entry(&self, search_idx: u32) -> Option<(LogEntry, usize)> {
        if let Ok(i) = self
            .buf
            .binary_search_by(|entry| entry.index.cmp(&search_idx))
        {
            println!("[LOG CACHE] HIT!");
            return Some((self.buf.get(i).unwrap().to_owned(), i));
        }

        println!("[LOG CACHE] MISS!");
        None
    }

    pub async fn get_entries_from(&self, _idx: u32, _max: u16) {
        todo!("get entries from")
    }

    pub async fn get_first_index_of_term(&self, _term: LogTerm, _start_idx: usize) -> Option<u32> {
        todo!("get first index");
    }
}
