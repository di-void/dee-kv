use dee_kv::{config, server, utils};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = utils::env::parse_cli_args()?;
    let cluster = config::parse_cluster_config(args)?;

    server::start(cluster).await?;

    Ok(())
}

// https://docs.rs/tokio/1.48.0/tokio/
