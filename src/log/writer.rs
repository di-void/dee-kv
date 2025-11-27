use anyhow::{Context, Result};

use crate::{
    LOG_FILE_DELTA_THRESH,
    log::file::{generate_file_name, open_file},
};

use super::file::{check_file_delta, create_file, get_log_files, validate_or_create_path};
use std::{
    fs::File,
    io::{BufWriter, Write},
    os::windows::fs::MetadataExt,
    path::{Path, PathBuf},
};

pub struct LogWriter {
    curr_file: BufWriter<File>,
    data_dir_path: PathBuf,
}

impl LogWriter {
    pub fn append(&mut self, payload: String, should_check: bool) -> Result<usize> {
        if should_check {
            let f_meta = self.curr_file.get_ref().metadata()?;
            if check_file_delta(f_meta.file_size()) >= LOG_FILE_DELTA_THRESH {
                let fname = generate_file_name();
                let fh = create_file(&fname, &self.data_dir_path).with_context(|| {
                    format!(
                        "Failed to create new file at: {:?}/{:?}",
                        &self.data_dir_path.to_str().unwrap(),
                        fname
                    )
                })?;

                self.curr_file = BufWriter::new(fh);
            }
        }

        let bytes = self
            .curr_file
            .write(payload.as_bytes())
            .with_context(|| format!("Failed to append to log writing payload: {:?}", payload))?;

        Ok(bytes)
    }

    pub fn from_data_dir(dir_name: &str) -> Result<Self> {
        let path = Path::new(dir_name);
        let _ = validate_or_create_path(path)?;
        let files = get_log_files(path)?;
        if files.len() == 0 {
            let fname = generate_file_name();
            let fh = create_file(&fname, path).with_context(|| {
                format!(
                    "Failed to create new file at: {:?}/{:?}",
                    path.to_str().unwrap(),
                    fname
                )
            })?;

            Ok(Self {
                curr_file: BufWriter::new(fh),
                data_dir_path: path.to_path_buf(),
            })
        } else {
            let latest = &files[files.len() - 1];
            let file: File;

            if check_file_delta(latest.meta.file_size()) >= LOG_FILE_DELTA_THRESH {
                let fname = generate_file_name();
                let fh = create_file(&fname, path).with_context(|| {
                    format!(
                        "Failed to create new file at: {:?}/{:?}",
                        path.to_str().unwrap(),
                        fname
                    )
                })?;

                file = fh;
            } else {
                let fh = open_file(&latest.file_path).with_context(|| {
                    format!("Failed to open file at path: {:?}", &latest.file_path)
                })?;
                file = fh;
            }

            Ok(Self {
                curr_file: BufWriter::new(file),
                data_dir_path: path.to_path_buf(),
            })
        }
    }
}
