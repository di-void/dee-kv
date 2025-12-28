pub mod env {
    use anyhow::Result;
    use std::{collections::HashMap, env};

    pub fn get_env_vars() -> HashMap<String, String> {
        let vars = env::vars();
        vars.collect::<HashMap<_, _>>()
    }

    pub fn parse_cli_args() -> Result<HashMap<String, String>> {
        let cli_args = env::args()
            .skip(1)
            .filter_map(|arg| {
                arg.strip_prefix("--")
                    .and_then(|kv| kv.split_once('='))
                    .map(|(k, v)| (k.to_string(), v.to_string()))
            })
            .collect::<HashMap<_, _>>();

        Ok(cli_args)
    }
}

use std::ops::Range;
pub fn get_random_value(range: Range<u16>) -> u16 {
    rand::random_range(range)
}

pub mod cluster {
    use std::time::Duration;

    pub fn get_random_election_timeout() -> Duration {
        let val = super::get_random_value(150..301);
        Duration::from_millis(val.into())
    }
}

pub mod file {
    use std::{
        fs::{File, OpenOptions},
        path::Path,
    };

    pub fn open_or_create_file(path: &Path) -> anyhow::Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        Ok(file)
    }

    pub fn read_file(path: &Path) -> anyhow::Result<Vec<u8>> {
        Ok(std::fs::read(path)?)
    }

    pub fn file_exists(path: &Path) -> bool {
        path.exists()
    }
}
