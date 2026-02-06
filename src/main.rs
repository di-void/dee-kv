use dee_kv::{
    ConsensusMessage, DATA_DIR, LogMessage,
    cluster::{
        self, CurrentNode, consensus,
        consensus_apply::{ApplyMsg, run_apply_worker},
    },
    log, server,
    state::Store,
    utils::env,
};
use tokio::{
    runtime::Runtime,
    sync::{mpsc, watch},
};

fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber for structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,dee_kv=debug")),
        )
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    let rt = Runtime::new()?;
    let args = env::parse_cli_args()?;
    let env_vars = env::get_env_vars();
    let cluster = cluster::config::parse_cluster_config(args, env_vars)?;
    let current_node = CurrentNode::from_meta(cluster.self_id)?;
    let (state, last_logs) = log::load_or_init(DATA_DIR)?;
    let rt_handle = rt.handle();

    rt.block_on(async move {
        use std::sync::Arc;
        use tokio::sync::RwLock;

        let rt = rt_handle.clone();
        let logs = last_logs;
        let (lw_tx, lw_rx) = mpsc::channel::<LogMessage>(5);
        let (apply_tx, apply_rx) = mpsc::channel::<ApplyMsg>(8);
        let (shd_tx, shd_rx) = watch::channel::<Option<()>>(None);
        let (csus_tx, csus_rx) = watch::channel(ConsensusMessage::Init);
        // initialize atomic last-log meta from on-disk logs before starting writer/server
        let (disk_term, disk_last_idx) = log::get_last_log_meta(&logs);
        log::init_last_log_meta(disk_term, disk_last_idx);

        let logs_cache = log::cache::LogCache::from_last_logs(logs);
        let logs_cache = Arc::new(RwLock::new(logs_cache));
        let lw_handle =
            log::writer::init_log_writer(current_node.term, lw_rx, Arc::clone(&logs_cache));
        let current_node = Arc::new(RwLock::new(current_node));
        let store = Arc::new(RwLock::new(Store::from_state(state)));

        tokio::spawn(run_apply_worker(
            Arc::clone(&current_node),
            Arc::clone(&store),
            apply_rx,
            shd_rx.clone(),
        ));

        tracing::info!(
            node_id = cluster.self_id,
            address = %cluster.self_address,
            cluster_name = %cluster.name,
            "Starting dee-kv node"
        );

        let server_handle = server::start(
            cluster.self_address.clone(),
            Arc::clone(&current_node),
            Arc::clone(&store),
            Arc::clone(&logs_cache),
            lw_tx.clone(),
            apply_tx.clone(),
            shd_tx.clone(),
            csus_tx.clone(),
        )
        .await;

        match server_handle {
            Ok(s) => {
                if let Err(_) = consensus::begin(
                    &cluster,
                    Arc::clone(&current_node),
                    apply_tx.clone(),
                    (lw_tx.clone(), shd_rx.clone(), csus_rx.clone()),
                    rt,
                )
                .await
                {
                    tracing::error!("Error while starting consensus");
                };

                s.await.unwrap();
                lw_handle.join().expect("log writer thread errored");
            }
            Err(e) => tracing::error!(error = ?e, "Error while starting server"),
        }
    });

    Ok(())
}

// https://docs.rs/tonic/latest/tonic/
// https://github.com/tokio-rs/prost
// https://github.com/hyperium/tonic/tree/master/examples
// https://docs.rs/tokio/latest/tokio/
// https://docs.rs/futures/latest/futures/index.html
