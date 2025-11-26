use std::fs::{Metadata, create_dir_all};
use std::path::Path;

use crate::MAX_LOG_FILE_SIZE;

pub fn generate_file_name() {
    //
}

pub fn create_file(name: String, parent_dir: &Path) {
    //
}

pub fn write_to_file(path: &Path) {
    //
}

pub fn validate_path(path: &Path) -> anyhow::Result<()> {
    match path.try_exists() {
        Ok(true) => Ok(()),
        Ok(false) => {
            println!("path: {:#?}, could not be verified", path);
            println!("Initiating manual attempt");

            if let Err(e) = create_dir_all(path) {
                println!("Couldn't create path: {:#?}", path);
                println!("Error: {:#?}", e);
                return Err(e.into());
            }

            Ok(())
        }
        Err(er) => {
            println!("path: {:#?}, could not be verified", path);
            println!("Error: {:#?}", er);
            println!("Initiating manual attempt");

            if let Err(e) = create_dir_all(path) {
                println!("Couldn't create path: {:#?}", path);
                println!("Error: {:#?}", e);
                return Err(e.into());
            }

            Ok(())
        }
    }
}

pub struct LogFileEntry<'entry> {
    pub file_id: u16,
    pub file_path: &'entry Path,
    pub meta: Metadata,
}

pub fn get_log_files(path: &Path) -> Vec<LogFileEntry<'_>> {
    // iterate through entries in the directory
    // filter by .aof files
    // map to their parsed file names
    // then sort by their file names
    todo!("get log files")
}

pub fn check_file_delta(file_size: u64) -> u8 {
    let p = file_size / MAX_LOG_FILE_SIZE * 100;
    p as u8
}
