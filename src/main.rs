use tonic::{Request, Response, Status};

pub mod hello {
    tonic::include_proto!("hello");
}

fn main() {
    println!("Hello world");
}

// https://docs.rs/tonic/latest/tonic/
// https://github.com/tokio-rs/prost
// https://github.com/hyperium/tonic/tree/master/tonic-prost-build
// https://docs.rs/tokio/1.48.0/tokio/
