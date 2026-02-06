use crate::{LogIndex, log::file::get_entry_from_disk, serde::LogEntry};

// log cache
pub struct LogCache {
    buf: Vec<LogEntry>,
    size: usize,
    resize_offset: LogIndex,
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
            resize_offset: 0,
        }
    }

    pub fn push(&mut self, entry: LogEntry, size: usize) {
        self.buf.push(entry);
        self.size += size
    }

    async fn resize(&mut self) {}

    pub async fn get_entry(&self, search_idx: u32) -> Option<LogEntry> {
        match self
            .buf
            .binary_search_by(|entry| entry.index.cmp(&search_idx))
        {
            Ok(i) => Some(self.buf.get(i).unwrap().to_owned()),
            Err(_) => {
                let res = tokio::task::spawn_blocking(move || {
                    get_entry_from_disk(search_idx, None) // set the skip value to skip searching a number of log files
                });

                res.await.unwrap()
            }
        }
    }
    pub async fn get_entries_from(&self, idx: u32, max: u16) {}
    pub async fn get_first_index_of_term(&self, term: u32) {}
}
