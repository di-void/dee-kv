use crate::store_proto::{
    DeleteResponse, GetResponse, KeyRequest, PutRequest, PutResponse,
    store_service_server::StoreService as StoreSvc,
};
use crate::{
    LogWriterMsg, Op,
    store::{Store as KV, Types},
};
use tokio::sync::{RwLock, mpsc::Sender};
use tonic::{Request, Response, Status};
use std::sync::Arc;

pub struct StoreService {
    kv: Arc<RwLock<KV>>,
    log_writer: Sender<LogWriterMsg>,
}

impl StoreService {
    pub fn with_log_writer(store: Arc<RwLock<KV>>, tx: Sender<LogWriterMsg>) -> Self {
        Self {
            kv: store,
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

        self.log_writer
            .send(LogWriterMsg::LogAppend(Op::Put(
                kv.0.clone(),
                kv.1.clone().into(),
            )))
            .await
            .unwrap();

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

        let r = self.kv.read().await;
        let value = r.get(&key);
        match value {
            Some(v) => {
                let value: String = v.into();
                self.log_writer
                    .send(LogWriterMsg::LogAppend(Op::Delete(key.clone())))
                    .await
                    .unwrap();
                Ok(Response::new(DeleteResponse { key, value }))
            }
            _ => Err(Status::invalid_argument(format!(
                "Key: '{key}' doesn't exist"
            ))),
        }
    }
}
