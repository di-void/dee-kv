use tonic::{Request, Response, Status};

use store::store_server::{Store, StoreServer};
use store::{GetReply, GetRequest, PutReply, PutRequest};

pub mod store {
    tonic::include_proto!("store");
}

#[derive(Default)]
struct MyStore {}

#[tonic::async_trait]
impl Store for MyStore {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        todo!("do GET work")
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutReply>, Status> {
        todo!("do PUT work")
    }
}

pub async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let my_store = MyStore::default();

    tonic::transport::Server::builder()
        .add_service(StoreServer::new(my_store))
        .serve(addr)
        .await?;

    Ok(())
}
