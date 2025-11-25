use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::store::{Store as KV, Types};
use store_proto::store_server::{Store, StoreServer};
use store_proto::{DeleteReply, GetReply, KeyRequest, PutReply, PutRequest};

pub mod store_proto {
    tonic::include_proto!("store");
}

#[derive(Default)]
struct StoreService {
    kv: RwLock<KV>, // in-mem kv
}

#[tonic::async_trait]
impl Store for StoreService {
    async fn get(&self, request: Request<KeyRequest>) -> Result<Response<GetReply>, Status> {
        let msg = request.into_inner();
        let key = msg.key;

        let r = self.kv.read().await;
        let value = r.get(&key);
        if let Some(v) = value {
            match v {
                Types::String(s) => Ok(Response::new(GetReply { key, value: s })),
            }
        } else {
            Err(Status::invalid_argument(format!("{key} doesn't exist")))
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutReply>, Status> {
        let msg = request.into_inner();
        let kv = (msg.key, msg.value);

        let mut w = self.kv.write().await;
        w.set((&kv.0, kv.1.clone().into()));

        Ok(Response::new(PutReply {
            key: kv.0,
            value: kv.1,
        }))
    }

    async fn delete(&self, request: Request<KeyRequest>) -> Result<Response<DeleteReply>, Status> {
        let msg = request.into_inner();
        let key = msg.key;

        let mut w = self.kv.write().await;
        if let Some(v) = w.get(&key) {
            match v {
                Types::String(value) => {
                    w.delete(&key);
                    Ok(Response::new(DeleteReply { key, value }))
                }
            }
        } else {
            Err(Status::invalid_argument(format!("{key} doesn't exist")))
        }
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
