use dee_kv::{server, utils};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // server::start().await?;
    let args = utils::env::parse_cli_args();
    println!("CLI args: {:?}", args);

    Ok(())
}

// https://docs.rs/tokio/1.48.0/tokio/
