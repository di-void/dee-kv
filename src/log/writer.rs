use crate::{
    LOG_FILE_BUF_MAX, LOG_FILE_DELTA_THRESH,
    log::file::{
        CheckStatus, check_or_create_file, create_file, generate_file_name, get_log_files,
        open_file, validate_or_create_path,
    },
};
use anyhow::{Context, Result};
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
            if let CheckStatus::Over(fh) = check_or_create_file(
                f_meta.file_size(),
                LOG_FILE_DELTA_THRESH,
                &self.data_dir_path,
            )? {
                self.curr_file = BufWriter::new(fh);
            };
        }

        let bytes = self
            .curr_file
            .write(payload.as_bytes())
            .with_context(|| format!("Failed to append to log writing payload: {:?}", payload))?;

        Ok(bytes)
    }

    pub fn from_data_dir(dir_path: &str) -> Result<Self> {
        let path = Path::new(dir_path);
        let _ = validate_or_create_path(path)?; // parent path
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
                curr_file: BufWriter::with_capacity(LOG_FILE_BUF_MAX.into(), fh),
                data_dir_path: path.to_path_buf(),
            })
        } else {
            let latest = &files[files.len() - 1];
            let file: File;

            let res = check_or_create_file(latest.meta.file_size(), LOG_FILE_DELTA_THRESH, path)?;
            match res {
                CheckStatus::Good => {
                    let fh = open_file(&latest.file_path).with_context(|| {
                        format!("Failed to open file at path: {:?}", &latest.file_path)
                    })?;

                    file = fh;
                }
                CheckStatus::Over(fh) => {
                    file = fh;
                }
            }

            Ok(Self {
                curr_file: BufWriter::with_capacity(LOG_FILE_BUF_MAX.into(), file),
                data_dir_path: path.to_path_buf(),
            })
        }
    }
}
