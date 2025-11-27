use anyhow::{Context, Result};
use std::ffi::OsStr;
use std::fs::{Metadata, create_dir_all, read_dir};
use std::path::{Path, PathBuf};

use crate::{LOG_FILE_EXT, MAX_LOG_FILE_SIZE};

pub fn generate_file_name() {
    //
}

pub fn create_file(name: String, parent_dir: &Path) {
    //
}

pub fn write_to_file(path: &Path) {
    //
}

pub fn validate_path(path: &Path) -> Result<()> {
    match path.try_exists() {
        Ok(true) => Ok(()),
        _ => {
            println!(
                "path: {:#?}, could not be verified\n Initiating manual attempt",
                path
            );
            create_dir_all(path)
                .with_context(|| format!("Failed to create ./DATA directory at {:?}", path))?;
            Ok(())
        }
    }
}

pub struct LogFileEntry {
    pub file_id: u32,
    pub file_path: PathBuf,
    pub meta: Metadata,
}

pub fn get_log_files(path: &Path) -> Result<Vec<LogFileEntry>> {
    let rd = read_dir(path).unwrap();
    let mut entries = rd
        .map(|e| e.expect("Error getting next dir entry"))
        .filter(|e| {
            let path = e.path();
            let p = path.extension().unwrap_or(OsStr::new(""));
            p == LOG_FILE_EXT
        })
        .map(|e| {
            let file_name = e.file_name();
            let path = e.path();
            let file_id = file_name.to_str().unwrap().parse::<u32>().unwrap();
            let meta = e
                .metadata()
                .expect(&format!("Failed to get file metatadata: {:?}", path));

            LogFileEntry {
                file_id,
                file_path: path,
                meta,
            }
        })
        .collect::<Vec<_>>();

    // then sort by their file ids
    entries.sort_by(|a, b| a.file_id.cmp(&b.file_id));
    Ok(entries)
}

pub fn check_file_delta(file_size: u64) -> u8 {
    let p = file_size / MAX_LOG_FILE_SIZE * 100;
    p as u8
}
