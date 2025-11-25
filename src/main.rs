use dee_kv::server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    server::start().await?;

    Ok(())
}

// https://docs.rs/tokio/1.48.0/tokio/
