// log cache
pub struct LogCache {
    buf: Vec<u8>,
}

impl LogCache {
    pub async fn get_entry(&self, idx: u32) {}
    pub async fn get_entries_from(&self, idx: u32, max: u16) {}
    pub async fn get_first_index_of_term(&self, term: u32) {}
}
