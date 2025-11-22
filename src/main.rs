use dee_kv::server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    server::start().await?;

    Ok(())
}

// https://docs.rs/tonic/latest/tonic/
// https://github.com/tokio-rs/prost
// https://docs.rs/tokio/1.48.0/tokio/
