use dee_kv::{cluster, server, utils};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = utils::env::parse_cli_args()?;
    let cluster = cluster::config::parse_cluster_config(args)?;

    server::start(cluster).await?;

    Ok(())
}
