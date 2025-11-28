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
use store_proto::store_server::{Store, StoreServer};
use store_proto::{DeleteReply, GetReply, KeyRequest, PutReply, PutRequest};

pub mod store_proto {
    tonic::include_proto!("store");
}

struct StoreService {
    kv: RwLock<KV>, // in-mem kv
    w_tx: Sender<ChannelMessage>,
}

impl StoreService {
    fn from_sender(tx: Sender<ChannelMessage>) -> Self {
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

pub async fn start() -> anyhow::Result<()> {
    let addr = "[::1]:50051".parse()?;
    let (tx, rx) = mpsc::channel::<ChannelMessage>(5); // backpressure?
    let my_store = StoreService::from_sender(tx);

    let t_handle = setup_log_writer(rx); // setup 'writer' thread

    println!("Server is starting...");

    tonic::transport::Server::builder()
        .add_service(StoreServer::new(my_store))
        .serve(addr)
        .await?;

    t_handle.join().unwrap(); // join writer
    Ok(())
}

// https://docs.rs/tonic/latest/tonic/
// https://github.com/tokio-rs/prost
