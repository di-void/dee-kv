use anyhow::{Context, Error, Result};

use super::{Cluster, ClusterConfig, Node};
use std::{collections::HashMap, fs, path::Path};

pub fn parse_cluster_config(cli_args: HashMap<String, String>) -> Result<Cluster> {
    if !cli_args.contains_key("config") || !cli_args.contains_key("id") {
        return Err(Error::msg(
            "--config (config file path) and --id (node id) are required arguments",
        ))
        .with_context(|| format!("Error while parsing cluster config"));
    }

    let id = cli_args
        .get("id")
        .unwrap_or(&String::from("0"))
        .parse::<u8>()
        .with_context(|| format!("Failed to parse arg: 'id'"))?;
    let s = String::new();
    let config = cli_args.get("config").unwrap_or(&s);
    if id == 0 {
        return Err(Error::msg("node id can't be 0"))
            .with_context(|| format!("Error while parsing cluster config"));
    };

    let config_path = Path::new(config).canonicalize()?;
    let config_file_contents = fs::read(config_path)?;
    let cluster_config = serde_json::from_slice::<ClusterConfig>(&config_file_contents)
        .with_context(|| format!("Failed to parse cluster config file"))?;

    let ClusterConfig {
        cluster_name,
        nodes,
    } = cluster_config;
    let mut peers: Vec<Node> = vec![];
    let mut self_id: u8 = 0;
    let mut self_address: String = String::from("");
    for node in nodes {
        if node.id == id {
            self_id = node.id;
            self_address = node.address;
        } else {
            peers.push(node);
        }
    }

    Ok(Cluster {
        name: cluster_name,
        self_id,
        self_address,
        peers,
    })
}
