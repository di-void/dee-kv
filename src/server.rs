use tokio::sync::{
    RwLock,
    mpsc::{self, Sender},
};
use tonic::{Request, Response, Status};

use crate::{
    ChannelMessage, Op,
    log::setup_log_writer,
    store::{Store as KV, Types},
};
use health_proto::health_check_server::{HealthCheck, HealthCheckServer};
use health_proto::{PingReply, PingRequest};
use store_proto::store_server::{Store, StoreServer};
use store_proto::{DeleteReply, GetReply, KeyRequest, PutReply, PutRequest};

pub mod store_proto {
    tonic::include_proto!("store");
}

pub mod health_proto {
    tonic::include_proto!("health");
}

struct StoreService {
    kv: RwLock<KV>, // in-mem kv
    w_tx: Sender<ChannelMessage>,
}

#[derive(Default)]
struct HealthService {}

#[tonic::async_trait]
impl HealthCheck for HealthService {
    async fn ping(&self, _r: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        Ok(Response::new(PingReply {}))
    }
}

impl StoreService {
    fn with_sender(tx: Sender<ChannelMessage>) -> Self {
        Self {
            kv: Default::default(),
            w_tx: tx,
        }
    }
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
            Err(Status::invalid_argument(format!(
                "Key: '{key}' doesn't exist"
            )))
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutReply>, Status> {
        let msg = request.into_inner();
        let kv = (msg.key, msg.value);
        let tx = self.w_tx.clone();

        let mut w = self.kv.write().await;
        tx.send(ChannelMessage::Append(Op::Put(
            kv.0.clone(),
            kv.1.clone().into(),
        )))
        .await
        .unwrap(); // append to log

        w.set((&kv.0, kv.1.clone().into()));

        Ok(Response::new(PutReply {
            key: kv.0,
            value: kv.1,
        }))
    }

    async fn delete(&self, request: Request<KeyRequest>) -> Result<Response<DeleteReply>, Status> {
        let msg = request.into_inner();
        let key = msg.key;
        let tx = self.w_tx.clone();

        let mut w = self.kv.write().await;
        if let Some(v) = w.get(&key) {
            match v {
                Types::String(value) => {
                    tx.send(ChannelMessage::Append(Op::Delete(key.clone())))
                        .await
                        .unwrap(); // append to log
                    w.delete(&key);
                    Ok(Response::new(DeleteReply { key, value }))
                }
            }
        } else {
            Err(Status::invalid_argument(format!(
                "Key: '{key}' doesn't exist"
            )))
        }
    }
}

use crate::cluster::Cluster;
pub async fn start(cluster_config: Cluster) -> anyhow::Result<()> {
    println!("Cluster: {:#?}", cluster_config);

    let addr = "[::1]:50051".parse()?;
    let (tx, rx) = mpsc::channel::<ChannelMessage>(5);
    let store_svc = StoreService::with_sender(tx);
    let health_svc = HealthService::default();

    let th = setup_log_writer(rx);

    println!("Server is starting...");

    tonic::transport::Server::builder()
        .add_service(HealthCheckServer::new(health_svc))
        .add_service(StoreServer::new(store_svc))
        .serve(addr)
        .await?;

    th.join().unwrap();
    Ok(())
}

// https://docs.rs/tonic/latest/tonic/
// https://github.com/tokio-rs/prost
// https://github.com/hyperium/tonic/tree/master/examples
