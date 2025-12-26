use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::{LogWriterMessage, cluster::Cluster};
use crate::{
    health_proto::health_check_service_server::HealthCheckServiceServer,
    services::{health::HealthCheckService, store::StoreService},
    store_proto::store_service_server::StoreServiceServer,
};

pub async fn start(
    cluster_config: &Cluster,
    lw_tx: mpsc::Sender<LogWriterMessage>,
    sd_tx: watch::Sender<Option<()>>,
) -> anyhow::Result<JoinHandle<()>> {
    println!("{:#?}", cluster_config);

    let addr = cluster_config.self_address.clone();

    let handle = tokio::spawn(async move {
        println!("Server is listening on {addr}");

        let store_svc = StoreService::with_log_writer(lw_tx.clone());
        let health_svc = HealthCheckService::default();

        if let Err(e) = tonic::transport::Server::builder()
            .add_service(HealthCheckServiceServer::new(health_svc))
            .add_service(StoreServiceServer::new(store_svc))
            .serve_with_shutdown(addr, async {
                match shutdown_server(lw_tx, sd_tx).await {
                    Ok(_) => (),
                    Err(e) => println!("Error while shutting down server: {:?}", e),
                };
            })
            .await
        {
            println!("Failed to start server: {:?}", e);
        }
    });

    Ok(handle)
}

#[cfg(windows)]
async fn shutdown_server(
    lw_tx: mpsc::Sender<LogWriterMessage>,
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
    lw_tx: mpsc::Sender<LogWriterMessage>,
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
    lw_tx: &mpsc::Sender<LogWriterMessage>,
    s_tx: &watch::Sender<Option<()>>,
) -> anyhow::Result<()> {
    lw_tx.send(LogWriterMessage::ShutDown).await?; // shutdown log writer
    if let Err(e) = s_tx.send(Some(())) {
        println!("Failed to send shutdown message: {:?}", e);
    };

    Ok(())
}

// https://docs.rs/tonic/latest/tonic/
// https://github.com/tokio-rs/prost
// https://github.com/hyperium/tonic/tree/master/examples
// https://docs.rs/tokio/1.48.0/tokio/
