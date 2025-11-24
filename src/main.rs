use dee_kv::server;

#[tokio::main]
async fn main() {
    match server::start().await {
        Err(e) => {
            dbg!("Error while shutting down server: {}", e);
        }
        _ => println!("Server shutdown successfully!"),
    }
}

// https://docs.rs/tokio/1.48.0/tokio/
