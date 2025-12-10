use std::sync::Arc;

use tokio::{
    runtime::Handle,
    sync::{
        RwLock,
        mpsc::{self, Sender},
        watch,
    },
};
use tonic::{Request, Response, Status};

use crate::health_proto::{
    PingReply, PingRequest,
    health_check_server::{HealthCheck, HealthCheckServer},
};
use crate::store_proto::{
    DeleteReply, GetReply, KeyRequest, PutReply, PutRequest,
    store_server::{Store, StoreServer},
};
use crate::{
    ChannelMessage, Op,
    cluster::health::{init_peers, start_heartbeat_loop},
    log::start_log_writer,
    store::{Store as KV, Types},
};

struct StoreService {
    kv: RwLock<KV>, // in-mem kv
    w_tx: Sender<ChannelMessage>,
}

#[derive(Default)]
struct HealthService {}

#[tonic::async_trait]
impl HealthCheck for HealthService {
    async fn ping(&self, _r: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        println!("Received Ping Request. Sending reply..");
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
        tx.send(ChannelMessage::LogAppend(Op::Put(
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
                    tx.send(ChannelMessage::LogAppend(Op::Delete(key.clone())))
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
pub async fn start(cluster_config: Cluster, rt: &Handle) -> anyhow::Result<()> {
    println!("Cluster: {:#?}", cluster_config);

    let addr = cluster_config.self_address.clone();
    let (s_tx, s_rx) = watch::channel::<Option<()>>(None);
    let (tx, rx) = mpsc::channel::<ChannelMessage>(5);

    let lw_handle = start_log_writer(rx);

    let task = tokio::spawn(async move {
        println!("Server is listening on {addr}");

        let store_svc = StoreService::with_sender(tx.clone());
        let health_svc = HealthService::default();

        if let Err(e) = tonic::transport::Server::builder()
            .add_service(HealthCheckServer::new(health_svc))
            .add_service(StoreServer::new(store_svc))
            .serve_with_shutdown(addr, async {
                match shutdown_server(tx, s_tx).await {
                    Ok(_) => (),
                    Err(e) => println!("Error while shutting down server: {:?}", e),
                };
            })
            .await
        {
            println!("Failed to start server: {:?}", e);
        }
    });

    let p_table = init_peers(&cluster_config.peers).await?;
    let pt_arc = Arc::new(p_table);
    let hb_handle = start_heartbeat_loop(Arc::clone(&pt_arc), rt.clone(), s_rx.clone()).await;

    task.await.unwrap();
    if hb_handle.is_some() {
        hb_handle.unwrap().join().unwrap();
    }
    lw_handle.join().unwrap();
    Ok(())
}

#[cfg(windows)]
async fn shutdown_server(
    lw_tx: mpsc::Sender<ChannelMessage>,
    s_tx: watch::Sender<Option<()>>,
) -> anyhow::Result<()> {
    use tokio::signal;
    let mut ctrlc = signal::windows::ctrl_c()?;
    ctrlc.recv().await;
    println!("Shutting down server...");

    Ok(issue_shutdown(&lw_tx, &s_tx).await?)
}

#[cfg(unix)]
async fn shutdown_server(
    lw_tx: mpsc::Sender<ChannelMessage>,
    s_tx: watch::Sender<Option<()>>,
) -> anyhow::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sig_term = signal(SignalKind::terminate())?;
    let mut sig_hup = signal(SignalKind::hangup())?;
    let mut sig_int = signal(SignalKind::interrupt())?;

    let res = tokio::select! {
        _ = sig_term.recv() => {
            println!("Received SIGTERM. Shutting down..");
            Ok(issue_shutdown(&lw_tx, &s_tx).await?)
        }
        _ = sig_hup.recv() => {
            println!("Received SIGHUP. Shutting down..");
            Ok(issue_shutdown(&lw_tx, &s_tx).await?)
        }
        _ = sig_int.recv() => {
            println!("Received SIGINT. Shutting down..");
            Ok(issue_shutdown(&lw_tx, &s_tx).await?)
        }
    };

    res
}

async fn issue_shutdown(
    lw_tx: &mpsc::Sender<ChannelMessage>,
    s_tx: &watch::Sender<Option<()>>,
) -> anyhow::Result<()> {
    lw_tx.send(ChannelMessage::ShutDown).await?; // shutdown log writer
    if let Err(e) = s_tx.send(Some(())) {
        println!("Failed to send shutdown message: {:?}", e);
    };

    Ok(())
}

// https://docs.rs/tonic/latest/tonic/
// https://github.com/tokio-rs/prost
// https://github.com/hyperium/tonic/tree/master/examples
// https://docs.rs/tokio/1.48.0/tokio/
