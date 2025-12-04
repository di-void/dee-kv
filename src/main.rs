use dee_kv::{cluster, server, utils::env};
use tokio::runtime::Runtime;

fn main() -> anyhow::Result<()> {
    let rt = Runtime::new()?;
    let args = env::parse_cli_args()?;
    let env_vars = env::get_env_vars();
    let cluster = cluster::config::parse_cluster_config(args, env_vars)?;
    let rt_handle = rt.handle();

    rt.block_on(async move {
        let rt = rt_handle.clone();
        if let Err(e) = server::start(cluster, &rt).await {
            println!("Failed to start server. Error: {:?}", e);
        }
    });

    Ok(())
}
