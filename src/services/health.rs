use std::sync::Arc;

use crate::{
    cluster::{Peer, PeersTable},
    health_proto::{
        PingReply, PingRequest, health_check_client::HealthCheckClient,
        health_check_server::HealthCheck,
    },
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status, transport::Channel};

#[derive(Default)]
pub struct HealthService {}

#[tonic::async_trait]
impl HealthCheck for HealthService {
    async fn ping(&self, _r: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        println!("Received Ping Request. Sending reply..");
        Ok(Response::new(PingReply {}))
    }
}

pub fn init_health_clients(
    pt: Arc<PeersTable>,
) -> Vec<(HealthCheckClient<Channel>, Arc<Mutex<Peer>>)> {
    let cp = pt
        .iter()
        .map(|p| {
            let guard = p.blocking_lock();
            let client = HealthCheckClient::new(guard.client.clone());
            drop(guard);

            (client, Arc::clone(p))
        })
        .collect();

    cp
}
