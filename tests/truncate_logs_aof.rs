use dee_kv::{log::truncate_logs, LOG_FILE_DELIM};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        let unique = format!(
            "{}_{}_{}",
            prefix,
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after unix epoch")
                .as_nanos()
        );

        let path = std::env::temp_dir().join(unique);
        fs::create_dir_all(&path).expect("temp dir should be created");

        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn put_entry_json(term: u16, index: u32, key: &str, value: &str) -> String {
    format!(
        "{{\"payload\":{{\"Put\":{{\"key\":\"{}\",\"value\":\"{}\"}}}},\"term\":{},\"index\":{}}}",
        key, value, term, index
    )
}

fn write_aof(path: &Path, entries: &[String]) {
    let mut body = entries.join(LOG_FILE_DELIM);
    body.push_str(LOG_FILE_DELIM);
    fs::write(path, body.as_bytes()).expect("aof file should be written");
}

#[test]
fn truncate_logs_truncates_target_aof_file_to_requested_index() {
    let tmp = TempDir::new("dee_kv_truncate_logs");

    let file1 = tmp.path().join("1000.aof");
    let file2 = tmp.path().join("2000.aof");

    let idx1 = put_entry_json(1, 1, "k1", "v1");
    let idx2 = put_entry_json(1, 2, "k2", "v2");
    let idx3 = put_entry_json(2, 3, "k3", "v3");
    let idx4 = put_entry_json(2, 4, "k4", "v4");

    write_aof(&file1, &[idx1.clone(), idx2.clone()]);
    write_aof(&file2, &[idx3.clone(), idx4.clone()]);

    let (new_writer, last_term, last_idx, old_paths) =
        truncate_logs(tmp.path(), 3).expect("truncate should succeed");

    drop(new_writer);

    assert_eq!(last_term, 2);
    assert_eq!(last_idx, 3);
    assert!(old_paths.is_empty());

    let expected_file2 = format!("{}{}", idx3, LOG_FILE_DELIM);
    let actual_file2 = fs::read_to_string(&file2).expect("target file should be readable");
    assert_eq!(actual_file2, expected_file2);

    let expected_file1 = format!("{}{}{}{}", idx1, LOG_FILE_DELIM, idx2, LOG_FILE_DELIM);
    let actual_file1 = fs::read_to_string(&file1).expect("older file should be readable");
    assert_eq!(actual_file1, expected_file1);
}
