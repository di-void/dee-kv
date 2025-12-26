use crate::{
    LOG_FILE_FLUSH_LIMIT, LOG_FILE_MAX_DELTA, META_FILE_FLUSH_LIMIT, META_FILE_PATH,
    log::file::{
        CheckStatus, check_file_size_or_create, generate_file_name, get_file_size, get_log_files,
        open_file, open_or_create_file, validate_or_create_dir,
    },
    utils::file as file_utils,
};
use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

pub struct LogWriter {
    curr_log_file: BufWriter<File>,
    meta_file: BufWriter<File>,
    data_dir_path: PathBuf,
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

    pub fn write_meta(&mut self, payload: &[u8]) -> Result<usize> {
        let bytes = self
            .meta_file
            .write(payload)
            .with_context(|| format!("Failed to write meta file writing: {:?}", payload))?;
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
            meta_file: BufWriter::with_capacity(META_FILE_FLUSH_LIMIT.into(), meta_file),
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

        match self.meta_file.flush() {
            Ok(_) => println!("[META FILE]: Flushed buffer successfully!"),
            Err(e) => println!("[META FILE]: An error occurred while flushing: {:?}", e),
        }
    }
}
