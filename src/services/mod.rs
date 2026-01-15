pub mod consensus;
pub mod health;
pub mod store;

use crate::cluster::{Peer, PeersTable};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;

pub trait GrpcClientWrapper<C = Self, Ch = Channel> {
    fn new_client(inner: Ch) -> C;
}

/// Create an Arc- and Mutex-wrapped gRPC client for each peer and pair it with that peer.
///
/// Each tuple in the returned vector contains an `Arc<Mutex<C>>` — a client constructed from the
/// peer's channel — and an `Arc<Mutex<Peer>>` referencing the same peer.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// // `PeersTable` and `Peer` are assumed to be available from the crate's cluster module,
/// // and `MyClient` is a type implementing `GrpcClientWrapper`.
/// let peers: Arc<crate::cluster::PeersTable> = Arc::new(Default::default());
/// // Replace `MyClient` with your concrete client type that implements `GrpcClientWrapper`.
/// let _clients = crate::create_custom_clients::<MyClient>(&peers);
/// ```
pub fn create_custom_clients<C: GrpcClientWrapper>(
    pt: &Arc<PeersTable>,
) -> Vec<(C, Arc<Mutex<Peer>>)> {
    let cp = pt
        .iter()
        .map(|p| {
            let guard = p.blocking_lock();
            let client = C::new_client(guard.channel.clone());
            drop(guard);

            (client, Arc::clone(p))
        })
        .collect();

    cp
}