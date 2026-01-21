use std::sync::Arc;
use tokio::{
    sync::{RwLock, mpsc, watch},
    task::JoinHandle,
};

use crate::{ConsensusMessage, LogWriterMsg, cluster::CurrentNode};
use crate::{
    consensus_proto::consensus_service_server::ConsensusServiceServer,
    health_proto::health_check_service_server::HealthCheckServiceServer,
    services::{consensus::ConsensusService, health::HealthCheckService, store::StoreService},
    store_proto::store_service_server::StoreServiceServer,
};

pub async fn start(
    addr: std::net::SocketAddr,
    current_node: Arc<RwLock<CurrentNode>>,
    lw_tx: mpsc::Sender<LogWriterMsg>,
    sd_tx: watch::Sender<Option<()>>,
    csus_tx: watch::Sender<ConsensusMessage>,
) -> anyhow::Result<JoinHandle<()>> {
    let handle = tokio::spawn(async move {
        tracing::info!(address = %addr, "Server is listening");

        let store_svc = StoreService::with_log_writer(lw_tx.clone());
        let health_svc = HealthCheckService::default();
        let consensus_svc =
            ConsensusService::with_state(Arc::clone(&current_node), lw_tx.clone(), csus_tx.clone());

        if let Err(e) = tonic::transport::Server::builder()
            .add_service(HealthCheckServiceServer::new(health_svc))
            .add_service(StoreServiceServer::new(store_svc))
            .add_service(ConsensusServiceServer::new(consensus_svc))
            .serve_with_shutdown(addr, async {
                match shutdown_server(lw_tx, sd_tx).await {
                    Ok(_) => (),
                    Err(e) => println!("Error while shutting down server: {:?}", e),
                };
            })
            .await
        {
            tracing::error!(error = ?e, "Failed to start server");
        }
    });

    Ok(handle)
}

#[cfg(windows)]
async fn shutdown_server(
    lw_tx: mpsc::Sender<LogWriterMsg>,
    s_tx: watch::Sender<Option<()>>,
) -> anyhow::Result<()> {
    use tokio::signal;
    let mut ctrlc = signal::windows::ctrl_c()?;
    ctrlc.recv().await;

    Ok(issue_shutdown(&lw_tx, &s_tx).await?)
}

#[cfg(unix)]
async fn shutdown_server(
    lw_tx: mpsc::Sender<LogWriterMsg>,
    s_tx: watch::Sender<Option<()>>,
) -> anyhow::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sig_term = signal(SignalKind::terminate())?;
    let mut sig_hup = signal(SignalKind::hangup())?;
    let mut sig_int = signal(SignalKind::interrupt())?;

    let res = tokio::select! {
        _ = sig_term.recv() => {
            tracing::info!("Received SIGTERM, shutting down server");
            Ok(issue_shutdown(&lw_tx, &s_tx).await?)
        }
        _ = sig_hup.recv() => {
            tracing::info!("Received SIGHUP, shutting down server");
            Ok(issue_shutdown(&lw_tx, &s_tx).await?)
        }
        _ = sig_int.recv() => {
            tracing::info!("Received SIGINT, shutting down server");
            Ok(issue_shutdown(&lw_tx, &s_tx).await?)
        }
    };

    res
}

async fn issue_shutdown(
    lw_tx: &mpsc::Sender<LogWriterMsg>,
    s_tx: &watch::Sender<Option<()>>,
) -> anyhow::Result<()> {
    lw_tx.send(LogWriterMsg::ShutDown).await?; // shutdown log writer
    if let Err(e) = s_tx.send(Some(())) {
        tracing::error!(error = ?e, "Failed to send shutdown message");
    };

    Ok(())
}
