use anyhow::{Context, Result};

use crate::log::file::{generate_file_name, open_file};

use super::file::{check_file_delta, create_file, get_log_files, validate_path};
use std::{
    fs::File,
    io::{BufWriter, Write},
    os::windows::fs::MetadataExt,
    path::Path,
};

pub struct LogWriter {
    curr_file: BufWriter<File>,
}

impl LogWriter {
    pub fn append(&mut self, payload: String) -> Result<usize> {
        let bytes = self
            .curr_file
            .write(payload.as_bytes())
            .with_context(|| format!("Failed to append to log writing payload: {:?}", payload))?;

        Ok(bytes)
    }

    pub fn from_data_dir(dir_name: &str) -> Result<Self> {
        let path = Path::new(dir_name);
        let _ = validate_path(path)?;
        let files = get_log_files(path)?;
        if files.len() == 0 {
            let new_file_name = generate_file_name();
            let fh = create_file(&new_file_name, path).with_context(|| {
                format!(
                    "Failed to create new file at: {:?}/{:?}",
                    path, new_file_name
                )
            })?;

            Ok(Self {
                curr_file: BufWriter::new(fh),
            })
        } else {
            let latest = &files[files.len() - 1];
            let file: File;

            if check_file_delta(latest.meta.file_size()) >= 90 {
                let new_file_name = generate_file_name();
                let fh = create_file(&new_file_name, path).with_context(|| {
                    format!(
                        "Failed to create new file at: {:?}/{:?}",
                        path, new_file_name
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
            })
        }
    }
}
