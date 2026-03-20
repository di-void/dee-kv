use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{File, Metadata, OpenOptions, create_dir_all, read_dir},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use crate::{LOG_FILE_DELIM, LOG_FILE_EXT, LOG_FILE_MAX_DELTA};
use crate::{
    serde::{LogEntry, deserialize_entry},
    state::Types,
};

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

pub fn check_file_delta(file_size: u64, max_size: u64) -> u8 {
    let p = file_size / max_size * 100;
    p as u8
}

pub fn replay_log_file(
    file: LogFile,
    logs_map: &mut HashMap<String, Types>,
    buf: &mut Option<&mut Vec<(LogEntry, usize)>>,
) -> Result<()> {
    let file = open_file(&file.file_path)?;
    let file = BufReader::new(file);

    file.split(LOG_FILE_DELIM.as_bytes()[0]).for_each(|line| {
        let bytes = match line {
            Ok(bytes) => bytes,
            _ => return,
        };

        if bytes.is_empty() {
            return;
        }

        let log = match deserialize_entry::<LogEntry>(&bytes) {
            Ok(log) => log,
            _ => return,
        };

        if log.index == 0 {
            return;
        }

        use crate::serde::Payload;
        match log.payload {
            Payload::Put { ref key, ref value } => {
                logs_map.insert(key.clone(), value.clone().into());
            }
            Payload::Delete { ref key } => {
                logs_map.remove(key);
            }
        }

        if buf.is_some() {
            buf.take().unwrap().push((log, bytes.len()));
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
    max: u64,
    parent_path: &Path,
) -> Result<CheckStatus> {
    if check_file_delta(file_size, max) >= LOG_FILE_MAX_DELTA {
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
