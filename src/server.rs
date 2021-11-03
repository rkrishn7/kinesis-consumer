mod proto;
mod storage;

use tonic::{transport::Server, Request, Response, Status};

use futures_core::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::ReceiverStream;

use storage::dynamo_manager;

use proto::consumer_server::{Consumer, ConsumerServer};
use proto::{
    CheckpointConsumerRequest, CheckpointConsumerResponse, GetRecordsRequest, GetRecordsResponse,
    ShutdownConsumerRequest, ShutdownConsumerResponse,
};

#[derive(Debug, Default)]
pub struct KinesisConsumerService {}

#[tonic::async_trait]
impl Consumer for KinesisConsumerService {
    type GetRecordsStream = ReceiverStream<Result<GetRecordsResponse, Status>>;

    async fn get_records(
        &self,
        request: tonic::Request<GetRecordsRequest>,
    ) -> Result<tonic::Response<Self::GetRecordsStream>, tonic::Status> {
        unimplemented!()
    }
    async fn shutdown_consumer(
        &self,
        request: tonic::Request<ShutdownConsumerRequest>,
    ) -> Result<tonic::Response<ShutdownConsumerResponse>, tonic::Status> {
        unimplemented!()
    }
    async fn checkpoint_consumer(
        &self,
        request: tonic::Request<CheckpointConsumerRequest>,
    ) -> Result<tonic::Response<CheckpointConsumerResponse>, tonic::Status> {
        unimplemented!()
    }
}

/*
#[tonic::async_trait]
impl Greeter for MyGreeter {
    type GetRecordsStream = ReceiverStream<Result<DataRecord, Status>>;

    async fn get_records(
        &self,
        request: Request<KinesisStream>,
    ) -> Result<Response<Self::GetRecordsStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            for x in 0..10 {
                let id: String = (x as u8).to_string();

                tx.send(Ok(DataRecord {
                    id,
                })).await.unwrap();

                sleep(Duration::from_millis(1000)).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
*/

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let consumer = KinesisConsumerService::default();

    Server::builder()
        .add_service(ConsumerServer::new(consumer))
        .serve(addr)
        .await?;

    Ok(())
}
