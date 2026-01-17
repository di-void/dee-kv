use dee_kv::{
    ConsensusMessage, LogWriterMsg,
    cluster::{self, CurrentNode, consensus},
    log, server,
    utils::env,
};
use tokio::{
    runtime::Runtime,
    sync::{mpsc, watch},
};

fn main() -> anyhow::Result<()> {
    let rt = Runtime::new()?;
    let args = env::parse_cli_args()?;
    let env_vars = env::get_env_vars();
    let cluster = cluster::config::parse_cluster_config(args, env_vars)?;
    let current_node = CurrentNode::from_meta(cluster.self_id)?;
    let rt_handle = rt.handle();

    rt.block_on(async move {
        use std::sync::Arc;
        use tokio::sync::RwLock;

        let rt = rt_handle.clone();
        let (lw_tx, lw_rx) = mpsc::channel::<LogWriterMsg>(5);
        let (shd_tx, shd_rx) = watch::channel::<Option<()>>(None);
        let (csus_tx, csus_rx) = watch::channel(ConsensusMessage::Init);
        // initialize atomic last-log meta from on-disk logs before starting writer/server
        let (disk_term, disk_last_idx) = log::get_log_meta();
        log::init_last_log_meta(disk_term, disk_last_idx);
        let lw_handle = log::init_log_writer(current_node.term, lw_rx);
        let current_node = Arc::new(RwLock::new(current_node));

        let server_handle = server::start(
            cluster.self_address.clone(),
            Arc::clone(&current_node),
            lw_tx.clone(),
            shd_tx.clone(),
            csus_tx.clone(),
        )
        .await;

        match server_handle {
            Ok(s) => {
                let _ = consensus::begin(
                    &cluster,
                    Arc::clone(&current_node),
                    (lw_tx.clone(), shd_rx.clone(), csus_rx.clone()),
                    rt,
                )
                .await;

                s.await.unwrap();
                lw_handle.join().expect("log writer thread errored");
            }
            Err(e) => println!("Error while starting server: {:?}", e),
        }
    });

    Ok(())
}

// https://docs.rs/tonic/latest/tonic/
// https://github.com/tokio-rs/prost
// https://github.com/hyperium/tonic/tree/master/examples
// https://docs.rs/tokio/latest/tokio/
// https://docs.rs/futures/latest/futures/index.html
