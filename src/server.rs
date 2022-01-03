use tonic::{Response, Status};

use crate::storage::KinesisStorageBackend;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::proto::consumer_service_server::ConsumerService;
use crate::proto::{
    CheckpointLeaseRequest, GetRecordsRequest, GetRecordsResponse,
    InitializeRequest,
};

use crate::kinesis_butler::KinesisButler;

use crate::record_processor::RecordProcessorRegister;

#[tonic::async_trait]
impl<T, U> ConsumerService for KinesisButler<T, U>
where
    T: KinesisStorageBackend + Send + Sync + Clone + 'static,
    U: RecordProcessorRegister + Send + Sync + Clone + 'static,
{
    type GetRecordsStream = ReceiverStream<Result<GetRecordsResponse, Status>>;

    async fn get_records(
        &self,
        request: tonic::Request<GetRecordsRequest>,
    ) -> Result<tonic::Response<Self::GetRecordsStream>, tonic::Status> {
        let remote_addr = request.remote_addr().unwrap();
        let message = request.into_inner();
        let (tx, rx) =
            mpsc::channel::<Result<GetRecordsResponse, tonic::Status>>(100);

        for stream in message.streams.clone() {
            if !self
                .record_processor_register
                .is_registered(message.app_name.clone(), stream, remote_addr)
                .await
            {
                return Err(tonic::Status::internal("Not registered"));
            }
        }

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
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let message = request.into_inner();
        let lease = message
            .lease
            .ok_or(tonic::Status::invalid_argument("Lease not specified"))?;

        self.storage_backend
            .checkpoint_lease(
                &lease.try_into().unwrap(),
                message.sequence_number.as_str(),
            )
            .await
            .map_err(|_| {
                tonic::Status::internal(
                    "Error occured while checkpointing lease",
                )
            })?;

        Ok(Response::new(()))
    }

    async fn initialize(
        &self,
        request: tonic::Request<InitializeRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let remote_addr = request.remote_addr().unwrap();
        let message = request.into_inner();

        self.record_processor_register
            .register(message.app_name, message.streams, remote_addr)
            .await;

        Ok(Response::new(()))
    }

    async fn shutdown(
        &self,
        request: tonic::Request<()>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let remote_addr = request.remote_addr().unwrap();

        self.record_processor_register.remove(remote_addr).await;

        Ok(Response::new(()))
    }
}
