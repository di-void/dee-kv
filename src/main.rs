use dee_kv::{LOOPBACK_NET_INT_STRING, cluster, server, utils::env};
use tokio::runtime::Runtime;

fn main() -> anyhow::Result<()> {
    let rt = Runtime::new()?;
    let args = env::parse_cli_args()?;
    let env_vars = env::get_env_vars();
    let default_net_interface = String::from(LOOPBACK_NET_INT_STRING);
    let net_int = env_vars
        .get("NET_INTERFACE")
        .unwrap_or(&default_net_interface);
    let cluster = cluster::config::parse_cluster_config(args)?;
    let rt_handle = rt.handle();

    rt.block_on(async move {
        let rt = rt_handle.clone();
        if let Err(e) = server::start(cluster, &rt, net_int).await {
            println!("Failed to start server. Error: {:?}", e);
        }
    });

    Ok(())
}
