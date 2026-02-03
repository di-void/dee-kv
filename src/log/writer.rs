use crate::{
    log::file::{
        check_file_size_or_create, generate_file_name, get_file_size, get_log_files, open_file,
        open_or_create_file, validate_or_create_dir, CheckStatus,
    },
    utils::file as file_utils,
    LOG_FILE_FLUSH_LIMIT, LOG_FILE_MAX_DELTA, META_BUF_CAPACITY, META_FILE_FLUSH_WRITES,
    META_FILE_PATH,
};
use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// A buffer for meta writes that overwrites its contents on each write,
/// grows dynamically as needed, and periodically flushes to disk by overwriting the file.
pub struct MetaBuffer {
    file: File,
    buffer: Vec<u8>,
    len: usize,
    write_count: u16,
    flush_threshold: u16,
}

impl MetaBuffer {
    pub fn new(file: File, capacity: usize, flush_threshold: u16) -> Self {
        Self {
            file,
            buffer: vec![0u8; capacity],
            len: 0,
            write_count: 0,
            flush_threshold,
        }
    }

    /// Overwrites the buffer with the given payload, resizing if necessary.
    pub fn write(&mut self, payload: &[u8]) -> usize {
        if payload.len() > self.buffer.len() {
            self.buffer.resize(payload.len(), 0);
        }
        let bytes_to_write = payload.len();
        self.buffer[..bytes_to_write].copy_from_slice(payload);
        self.len = bytes_to_write;
        self.write_count += 1;
        bytes_to_write
    }

    /// Returns true if the write count has reached the flush threshold.
    pub fn should_flush(&self) -> bool {
        self.write_count >= self.flush_threshold
    }

    /// Flushes the buffer to disk by seeking to the start and overwriting.
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&self.buffer[..self.len])?;
        self.file.set_len(self.len as u64)?; // Truncate file to current content size
        self.file.sync_all()?;
        self.write_count = 0;
        Ok(())
    }
}

pub struct LogWriter {
    pub(crate) curr_log_file: BufWriter<File>,
    meta_buffer: MetaBuffer,
    pub(crate) data_dir_path: PathBuf,
}

impl LogWriter {
    pub fn append_log(&mut self, payload: &[u8], should_check: bool) -> Result<usize> {
        if should_check {
            let f_meta = self.curr_log_file.get_ref().metadata()?;
            if let CheckStatus::Over(fh) = check_file_size_or_create(
                get_file_size(&f_meta),
                LOG_FILE_MAX_DELTA,
                &self.data_dir_path,
            )? {
                self.curr_log_file = BufWriter::new(fh);
            };
        }

        let bytes = self
            .curr_log_file
            .write(payload)
            .with_context(|| format!("Failed to append to log writing payload: {:?}", payload))?;

        Ok(bytes)
    }

    /// Writes metadata by overwriting the in-memory buffer.
    /// Flushes to disk after `META_FILE_FLUSH_WRITES` writes.
    pub fn write_meta(&mut self, payload: &[u8]) -> Result<usize> {
        let bytes = self.meta_buffer.write(payload);
        if self.meta_buffer.should_flush() {
            self.meta_buffer
                .flush()
                .with_context(|| "Failed to flush meta buffer to disk")?;
        }
        Ok(bytes)
    }

    pub fn with_data_dir(dir_path: &str) -> Result<Self> {
        let data_dir_path = Path::new(dir_path);
        let mut meta_path = data_dir_path.to_path_buf();
        meta_path.push(Path::new(META_FILE_PATH));
        let meta_file = file_utils::open_or_create_file(meta_path.as_path())?;
        let log_file: File;
        let _ = validate_or_create_dir(data_dir_path)?; // parent path
        let files = get_log_files(data_dir_path)?;

        if files.len() == 0 {
            let fname = generate_file_name();
            log_file = open_or_create_file(&fname, data_dir_path).with_context(|| {
                format!(
                    "Failed to create new file at: {:?}/{:?}",
                    data_dir_path.to_str().unwrap(),
                    fname
                )
            })?;
        } else {
            let latest = &files[files.len() - 1];
            let res = check_file_size_or_create(
                get_file_size(&latest.meta),
                LOG_FILE_MAX_DELTA,
                data_dir_path,
            )?;
            match res {
                CheckStatus::Good => {
                    let fh = open_file(&latest.file_path).with_context(|| {
                        format!("Failed to open file at path: {:?}", &latest.file_path)
                    })?;

                    log_file = fh;
                }
                CheckStatus::Over(fh) => log_file = fh,
            }
        }

        Ok(Self {
            curr_log_file: BufWriter::with_capacity(LOG_FILE_FLUSH_LIMIT.into(), log_file),
            meta_buffer: MetaBuffer::new(
                meta_file,
                META_BUF_CAPACITY.into(),
                META_FILE_FLUSH_WRITES,
            ),
            data_dir_path: data_dir_path.to_path_buf(),
        })
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        match self.curr_log_file.flush() {
            Ok(_) => println!("[LOG WRITER]: Flushed buffer successfully!"),
            Err(e) => println!("[LOG WRITER]: An error occurred while flushing: {:?}", e),
        }

        match self.meta_buffer.flush() {
            Ok(_) => println!("[META FILE]: Flushed buffer successfully!"),
            Err(e) => println!("[META FILE]: An error occurred while flushing: {:?}", e),
        }
    }
}
