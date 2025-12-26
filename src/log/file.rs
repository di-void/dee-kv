use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{File, Metadata, OpenOptions, create_dir_all, read_dir},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use crate::serde::{Log, LogOperation, Payload, deserialize_entry};
use crate::store::Types;
use crate::{LOG_FILE_DELIM, LOG_FILE_EXT, MAX_LOG_FILE_SIZE};

pub fn generate_file_name() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let mut timestamp_ms = now.as_millis().to_string();
    timestamp_ms.push_str(&format!(".{LOG_FILE_EXT}"));
    timestamp_ms
}

pub fn open_or_create_file(name: &str, parent_dir: &Path) -> Result<File> {
    let file_path = parent_dir.join(name);
    let file = OpenOptions::new()
        .append(true)
        .read(true)
        .create_new(true)
        .open(&file_path)?;
    Ok(file)
}

pub fn open_file(path: &Path) -> Result<File> {
    let fh = OpenOptions::new().append(true).read(true).open(path)?;
    Ok(fh)
}

pub fn validate_or_create_dir(path: &Path) -> Result<()> {
    match path.try_exists() {
        Ok(true) => Ok(()),
        _ => {
            println!(
                "path: {:#?}, could not be verified\n Initiating manual attempt",
                path
            );
            create_dir_all(path).with_context(|| format!("Failed to create path: {:?}", path))?;
            Ok(())
        }
    }
}

#[derive(Clone)]
pub struct LogFile {
    pub file_id: u64,
    pub file_path: PathBuf,
    pub meta: Metadata,
}

use std::fmt;
impl fmt::Debug for LogFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogFile")
            .field("file_id", &self.file_id)
            .field("file_path", &self.file_path)
            .finish()
    }
}

pub fn get_log_files(path: &Path) -> Result<Vec<LogFile>> {
    let _ = validate_or_create_dir(path)?;

    let rd = read_dir(path).unwrap();
    let mut entries = rd
        .map(|e| e.expect("Error getting next dir entry"))
        .filter_map(|e| {
            let path = e.path();
            let p = path.extension().unwrap_or(OsStr::new(""));

            if p != LOG_FILE_EXT {
                return None;
            }

            let file_name = path.file_stem().unwrap();
            let file_id = file_name.to_str().unwrap().parse::<u64>().unwrap();
            let meta = e
                .metadata()
                .expect(&format!("Failed to get file metatadata: {:?}", path));

            Some(LogFile {
                file_id,
                file_path: path,
                meta,
            })
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

pub fn replay_log_file(file: LogFile, hash: &mut HashMap<String, Types>) -> Result<()> {
    let file = open_file(&file.file_path)?;
    let file = BufReader::new(file);

    file.split(LOG_FILE_DELIM.as_bytes()[0]).for_each(|v| {
        let bytes = v.unwrap();
        let log = deserialize_entry::<Log>(&bytes).unwrap();

        let op = log.operation;
        match op {
            LogOperation::Put => {
                if let Payload::Put { key, value } = log.payload {
                    hash.insert(key, value.into());
                }
            }
            LogOperation::Delete => {
                if let Payload::Delete { key } = log.payload {
                    hash.remove(&key);
                }
            }
        }
    });

    Ok(())
}

pub enum CheckStatus {
    Good,
    Over(File),
}
pub fn check_file_size_or_create(
    file_size: u64,
    threshold: u8,
    parent_path: &Path,
) -> Result<CheckStatus> {
    if check_file_delta(file_size) >= threshold {
        let fname = generate_file_name();
        let fh = open_or_create_file(&fname, parent_path).with_context(|| {
            format!(
                "Failed to create new file at: {:?}/{:?}",
                parent_path.to_str().unwrap(),
                fname
            )
        })?;

        Ok(CheckStatus::Over(fh))
    } else {
        Ok(CheckStatus::Good)
    }
}

#[cfg(windows)]
pub fn get_file_size(meta: &Metadata) -> u64 {
    use std::os::windows::fs::MetadataExt;
    meta.file_size()
}

#[cfg(unix)]
pub fn get_file_size(meta: &Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    meta.size()
}
