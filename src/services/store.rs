use crate::cluster::CurrentNode;
use crate::store_proto::{
    DeleteResponse, GetResponse, KeyRequest, PutRequest, PutResponse,
    store_service_server::StoreService as StoreSvc,
};
use crate::{
    LogMessage, Op,
    state::{Store as KV, Types},
};
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc::Sender};
use tonic::{Request, Response, Status};

pub struct StoreService {
    current_node: Arc<RwLock<CurrentNode>>,
    kv: Arc<RwLock<KV>>,
    log_writer: Sender<LogMessage>,
}

impl StoreService {
    pub fn with_log_writer(
        store: Arc<RwLock<KV>>,
        tx: Sender<LogMessage>,
        current_node: Arc<RwLock<CurrentNode>>,
    ) -> Self {
        Self {
            kv: store,
            log_writer: tx,
            current_node,
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
        {
            let node = self.current_node.read().await;
            if !node.is_leader() {
                return Err(Status::aborted("follower node"));
            }
        }

        let msg = request.into_inner();
        let kv = (msg.key, msg.value);

        self.log_writer
            .send(LogMessage::Append {
                op: Op::Put(kv.0.clone(), kv.1.clone().into()),
                meta: None,
            })
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
        {
            let node = self.current_node.read().await;
            if !node.is_leader() {
                return Err(Status::aborted("follower node"));
            }
        }

        let msg = request.into_inner();
        let key = msg.key;

        let r = self.kv.read().await;
        let value = r.get(&key);
        match value {
            Some(v) => {
                let value: String = v.into();
                self.log_writer
                    .send(LogMessage::Append {
                        op: Op::Delete(key.clone()),
                        meta: None,
                    })
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
