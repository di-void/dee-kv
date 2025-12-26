use dee_kv::{
    ConsensusMessage, LogWriterMessage,
    cluster::{self, consensus},
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
    // get current node state
    let rt_handle = rt.handle();

    rt.block_on(async move {
        let rt = rt_handle.clone();
        let (lw_tx, lw_rx) = mpsc::channel::<LogWriterMessage>(5);
        let (shd_tx, shd_rx) = watch::channel::<Option<()>>(None);
        let (_csus_tx, csus_rx) = watch::channel(ConsensusMessage::Init);
        let lw_handle = log::init_log_writer(lw_rx);

        let server_handle = server::start(&cluster, lw_tx.clone(), shd_tx.clone()).await;
        match server_handle {
            Ok(s) => {
                let _ = consensus::begin(
                    &cluster,
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
