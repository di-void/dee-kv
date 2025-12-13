use dee_kv::{
    ChannelMessage,
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
    let rt_handle = rt.handle();

    rt.block_on(async move {
        let rt = rt_handle.clone();
        let (lw_tx, lw_rx) = mpsc::channel::<ChannelMessage>(5);
        let (sd_tx, _shutdown) = watch::channel::<Option<()>>(None);
        let lw_handle = log::init_log_writer(lw_rx);
        // we create a mpsc channel here for consensus
        // the server will take a producer to be used by the consensus service

        let server_handle = server::start(&cluster, lw_tx.clone(), sd_tx.clone()).await;
        match server_handle {
            Ok(s) => {
                // begin consensus
                let _ = consensus::begin(&cluster, lw_tx.clone(), sd_tx.clone(), rt).await;

                s.await.unwrap();
                lw_handle.join().expect("log writer thread errored");
            }
            Err(e) => println!("Error while starting server: {:?}", e),
        }
    });

    Ok(())
}
