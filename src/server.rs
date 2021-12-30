use tonic::{Response, Status};

use crate::storage::KinesisStorageBackend;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::proto::consumer_service_server::ConsumerService;
use crate::proto::{
    CheckpointLeaseRequest, CheckpointLeaseResponse, GetRecordsRequest,
    GetRecordsResponse,
};

use crate::kinesis_butler::KinesisButler;

use crate::connection_manager::ConnectionManager;

#[tonic::async_trait]
impl<T, U> ConsumerService for KinesisButler<T, U>
where
    T: KinesisStorageBackend + Send + Sync + Clone + 'static,
    U: ConnectionManager + Send + Sync + Clone + 'static,
{
    type GetRecordsStream = ReceiverStream<Result<GetRecordsResponse, Status>>;

    async fn get_records(
        &self,
        request: tonic::Request<GetRecordsRequest>,
    ) -> Result<tonic::Response<Self::GetRecordsStream>, tonic::Status> {
        let message = request.into_inner();
        let (tx, rx) =
            mpsc::channel::<Result<GetRecordsResponse, tonic::Status>>(100);

        let mut butler = self.clone();

        tokio::spawn(async move {
            if let Err(e) =
                butler.serve(message.app_name, message.streams, tx).await
            {
                println!("serving errorrr {}", e);
            }
            println!("connection dropped");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn checkpoint_lease(
        &self,
        request: tonic::Request<CheckpointLeaseRequest>,
    ) -> Result<tonic::Response<CheckpointLeaseResponse>, tonic::Status> {
        unimplemented!()
    }
}
