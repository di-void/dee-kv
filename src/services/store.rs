use crate::store_proto::{
    DeleteResponse, GetResponse, KeyRequest, PutRequest, PutResponse,
    store_service_server::StoreService as StoreSvc,
};
use crate::{
    LogWriterMessage, Op,
    store::{Store as KV, Types},
};
use tokio::sync::{RwLock, mpsc::Sender};
use tonic::{Request, Response, Status};

pub struct StoreService {
    kv: RwLock<KV>,
    log_writer: Sender<LogWriterMessage>,
}

impl StoreService {
    pub fn with_log_writer(tx: Sender<LogWriterMessage>) -> Self {
        Self {
            kv: Default::default(),
            log_writer: tx,
        }
    }
}

#[tonic::async_trait]
impl StoreSvc for StoreService {
    async fn get(&self, request: Request<KeyRequest>) -> Result<Response<GetResponse>, Status> {
        let msg = request.into_inner();
        let key = msg.key;

        let r = self.kv.read().await;
        let value = r.get(&key);
        if let Some(v) = value {
            match v {
                Types::String(s) => Ok(Response::new(GetResponse { key, value: s })),
            }
        } else {
            Err(Status::invalid_argument(format!(
                "Key: '{key}' doesn't exist"
            )))
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let msg = request.into_inner();
        let kv = (msg.key, msg.value);

        let mut w = self.kv.write().await;
        self.log_writer
            .send(LogWriterMessage::LogAppend(Op::Put(
                kv.0.clone(),
                kv.1.clone().into(),
            )))
            .await
            .unwrap();

        w.set((&kv.0, kv.1.clone().into()));

        Ok(Response::new(PutResponse {
            key: kv.0,
            value: kv.1,
        }))
    }

    async fn delete(
        &self,
        request: Request<KeyRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let msg = request.into_inner();
        let key = msg.key;

        let mut w = self.kv.write().await;
        if let Some(v) = w.get(&key) {
            match v {
                Types::String(value) => {
                    self.log_writer
                        .send(LogWriterMessage::LogAppend(Op::Delete(key.clone())))
                        .await
                        .unwrap();
                    w.delete(&key);
                    Ok(Response::new(DeleteResponse { key, value }))
                }
            }
        } else {
            Err(Status::invalid_argument(format!(
                "Key: '{key}' doesn't exist"
            )))
        }
    }
}
