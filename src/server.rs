use tonic::{Request, Response, Status};

use crate::store::{Store as KV, Types};
use store_proto::store_server::{Store, StoreServer};
use store_proto::{GetReply, GetRequest, PutReply, PutRequest};

pub mod store_proto {
    tonic::include_proto!("store");
}

#[derive(Default)]
struct StoreService {
    kv: KV, // in-mem kv
}

#[tonic::async_trait]
impl Store for StoreService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let msg = request.into_inner();
        let key = msg.key;

        let v = self.kv.get(&key);
        let value = if v.is_some() {
            match v.unwrap() {
                Types::String(s) => s,
            }
        } else {
            "".to_string()
        };

        Ok(Response::new(GetReply { key, value }))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutReply>, Status> {
        todo!("do PUT work")
    }
}

pub async fn start() -> anyhow::Result<()> {
    let addr = "[::1]:50051".parse()?;
    let my_store = StoreService::default();

    tonic::transport::Server::builder()
        .add_service(StoreServer::new(my_store))
        .serve(addr)
        .await?;

    Ok(())
}

// https://docs.rs/tonic/latest/tonic/
// https://github.com/tokio-rs/prost
