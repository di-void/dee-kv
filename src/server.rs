use std::sync::Arc;
use tokio::{
    runtime::Handle,
    sync::{mpsc, watch},
};

use crate::{
    ChannelMessage,
    cluster::{Cluster, config::init_peers, hearbeats::health::start_heartbeat_loop},
    log::init_log_writer,
};
use crate::{
    health_proto::health_check_server::HealthCheckServer,
    services::{health::HealthService, store::StoreService},
    store_proto::store_server::StoreServer,
};

pub async fn start(cluster_config: Cluster, rt: &Handle) -> anyhow::Result<()> {
    println!("{:#?}", cluster_config);

    let addr = cluster_config.self_address.clone();
    let (s_tx, shutdown) = watch::channel::<Option<()>>(None);

    let task = tokio::spawn(async move {
        println!("Server is listening on {addr}");
        let (lw_tx, lw_rx) = mpsc::channel::<ChannelMessage>(5);
        let lw_handle = init_log_writer(lw_rx);

        let store_svc = StoreService::with_log_writer(lw_tx.clone());
        let health_svc = HealthService::default();

        if let Err(e) = tonic::transport::Server::builder()
            .add_service(HealthCheckServer::new(health_svc))
            .add_service(StoreServer::new(store_svc))
            .serve_with_shutdown(addr, async {
                match shutdown_server(lw_tx, s_tx).await {
                    Ok(_) => (),
                    Err(e) => println!("Error while shutting down server: {:?}", e),
                };
            })
            .await
        {
            println!("Failed to start server: {:?}", e);
        }

        lw_handle
    });

    let p_table = init_peers(&cluster_config.peers).await?;
    let p_table = Arc::new(p_table);
    let hb_handle = start_heartbeat_loop(Arc::clone(&p_table), rt.clone(), shutdown.clone()).await;

    task.await.unwrap().join().unwrap();
    if hb_handle.is_some() {
        hb_handle.unwrap().join().unwrap();
    }
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
