use anyhow::{Context, Error, Result};

use crate::{LOCAL_HOST_IPV4, LOOPBACK_NET_INT_STRING, WILDCARD_IPV4, WILDCARD_NET_INT_STRING};

use super::{Cluster, ClusterConfig, Node};
use std::{collections::HashMap, fs, net::SocketAddr, path::Path};

pub fn parse_cluster_config(
    cli_args: HashMap<String, String>,
    env_vars: HashMap<String, String>,
) -> Result<Cluster> {
    if !cli_args.contains_key("config") || !cli_args.contains_key("id") {
        return Err(Error::msg(
            "--config (config file path) and --id (node id) are required arguments",
        ))
        .with_context(|| format!("Error while parsing cluster config"));
    }

    let default_net_interface = String::from(LOOPBACK_NET_INT_STRING);
    let net_int = env_vars
        .get("NET_INTERFACE")
        .unwrap_or(&default_net_interface);

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

    let self_address = parse_and_normalize_addr(self_address, net_int)?;

    Ok(Cluster {
        name: cluster_name,
        self_id,
        self_address,
        peers,
    })
}

fn parse_and_normalize_addr(addr: String, net_int: &str) -> Result<SocketAddr> {
    let port = addr.split(':').last().unwrap().parse::<u16>()?;

    let ip = match net_int {
        LOOPBACK_NET_INT_STRING => LOCAL_HOST_IPV4,
        WILDCARD_NET_INT_STRING => WILDCARD_IPV4,
        _ => unreachable!("received unknown net interface option"),
    };

    Ok(format!("{ip}:{port}").parse::<SocketAddr>()?)
}
