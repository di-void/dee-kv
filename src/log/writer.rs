use super::file::{check_file_delta, get_log_files, validate_path};
use std::{
    os::windows::fs::MetadataExt,
    path::{Path, PathBuf},
};

pub struct LogWriter {
    curr_write_path: PathBuf,
}

impl LogWriter {
    // add entry to log
    pub fn append(&self, payload: String) {
        todo!("write to the log")
    }

    pub fn from_data_dir(dir_name: &str) -> anyhow::Result<Self> {
        // get the path to the data directory
        let path = Path::new(dir_name);
        // validate the path
        let _ = validate_path(path)?;
        let files = get_log_files(path)?;
        let mut f_path: PathBuf;
        if files.len() == 0 {
            // create a new log file
            // set f_path to log file path
            todo!("create new log file")
        } else {
            let latest = &files[files.len() - 1];
            f_path = latest.file_path.clone();

            if check_file_delta(latest.meta.file_size()) >= 90 {
                // create new log file
                // set f_path variable to new log file path
                todo!("create new log file")
            }

            // then create a Path for said file
            Ok(Self {
                curr_write_path: f_path,
            })
        }
    }
}
