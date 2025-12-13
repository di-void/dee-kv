pub mod health;
pub mod store;

use crate::cluster::{Peer, PeersTable};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;

pub trait GrpcClientWrapper<C = Self, Ch = Channel> {
    fn new_client(inner: Ch) -> C;
}

pub fn create_custom_clients<C: GrpcClientWrapper>(
    pt: Arc<PeersTable>,
) -> Vec<(C, Arc<Mutex<Peer>>)> {
    let cp = pt
        .iter()
        .map(|p| {
            let guard = p.blocking_lock();
            let client = C::new_client(guard.client.clone());
            drop(guard);

            (client, Arc::clone(p))
        })
        .collect();

    cp
}
