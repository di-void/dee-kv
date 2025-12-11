use crate::health_proto::{PingReply, PingRequest, health_check_server::HealthCheck};
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct HealthService {}

#[tonic::async_trait]
impl HealthCheck for HealthService {
    async fn ping(&self, _r: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        println!("Received Ping Request. Sending reply..");
        Ok(Response::new(PingReply {}))
    }
}
