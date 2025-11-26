use crate::log::file::check_file_delta;

use super::file::{get_log_files, validate_path};
use std::{os::windows::fs::MetadataExt, path::Path};

pub struct LogWriter<'w> {
    curr_write_path: &'w Path,
}

impl<'w> LogWriter<'w> {
    // add entry to log
    pub fn append(&self, payload: String) {
        todo!("write to the log")
    }

    pub fn from_data_dir<'s: 'w>(dir_name: &'s str) -> anyhow::Result<Self> {
        // get the path to the data directory
        let path = Path::new(dir_name);
        // validate the path
        let _ = validate_path(path)?;
        let files = get_log_files(path);
        let latest = &files[files.len() - 1];
        let mut f_path = latest.file_path;

        if check_file_delta(latest.meta.file_size()) >= 90 {
            // create new log file
            // set f_path variable to new log file path
        }

        // then create a Path for said file
        Ok(Self {
            curr_write_path: Path::new(f_path),
        })
    }
}
