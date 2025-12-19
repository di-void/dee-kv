pub mod env {
    use anyhow::Result;
    use std::{collections::HashMap, env};

    pub fn get_env_vars() -> HashMap<String, String> {
        let vars = env::vars();
        vars.collect::<HashMap<_, _>>()
    }

    pub fn parse_cli_args() -> Result<HashMap<String, String>> {
        let mut args = env::args();
        args.next(); // skip exe

        let cli_args = args
            .filter_map(|a| {
                if !a.starts_with("--") {
                    return None;
                }

                let args_iter = &mut a[2..].split('=');
                let mut k: &str = "";
                let mut v: &str = "";
                for i in 0..=1 {
                    let item = args_iter.next().unwrap_or("");
                    if i == 0 {
                        k = item
                    } else {
                        v = item;
                    }
                }

                return Some((k.to_string(), v.to_string()));
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
