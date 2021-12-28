use tonic::{Response, Status};

use crate::receiver_aware_stream::ReceiverAwareStream;
use crate::storage::AsyncKinesisStorageBackend;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::proto::consumer_service_server::ConsumerService;
use crate::proto::{
    CheckpointLeaseRequest, CheckpointLeaseResponse, GetRecordsRequest,
    GetRecordsResponse,
};

use crate::service::KinesisConsumerService;

use crate::connection_manager::ConnectionManager;

#[tonic::async_trait]
impl<T, U> ConsumerService for KinesisConsumerService<T, U>
where
    T: AsyncKinesisStorageBackend + Send + Sync + Clone + 'static,
    U: ConnectionManager + Send + Sync + Clone + 'static,
{
    type GetRecordsStream =
        ReceiverAwareStream<Result<GetRecordsResponse, Status>>;

    async fn get_records(
        &self,
        request: tonic::Request<GetRecordsRequest>,
    ) -> Result<tonic::Response<Self::GetRecordsStream>, tonic::Status> {
        let remote_addr = request.remote_addr();
        let message = request.into_inner();
        let (record_sender, record_receiver) =
            mpsc::channel::<Result<GetRecordsResponse, tonic::Status>>(100);
        let (stream_dropped_notifier, stream_dropped_receiver) =
            oneshot::channel::<()>();

        self.refresh_consumer_leases(
            message.streams.clone(),
            message.app_name.clone(),
        )
        .await
        .map_err(|_| {
            tonic::Status::internal("Error refreshing consumer leases")
        })?;

        self.connection_manager
            .add_connection_for_streams(&message.streams, remote_addr);

        let butler = self.clone();

        let streams = message.streams;
        let app_name = message.app_name;

        tokio::spawn(async move {
            tokio::select! {
                _ = stream_dropped_receiver => {
                    println!("connection dropped");
                    butler.connection_manager
                        .remove_connection_for_streams(&streams, remote_addr)
                        .expect(
                            "Unable to remove connection.
                    This will result in an incorrect shard distribution across record
                    processors, which may result in shards being starved",
                        );
                },
                _ = butler.serve(app_name, streams.clone(), record_sender) => ()
            }
        });

        // TODO:
        // Fix connection manager, namespace by app_name

        Ok(Response::new(ReceiverAwareStream::new(
            record_receiver,
            Some(stream_dropped_notifier),
        )))
    }

    async fn checkpoint_lease(
        &self,
        request: tonic::Request<CheckpointLeaseRequest>,
    ) -> Result<tonic::Response<CheckpointLeaseResponse>, tonic::Status> {
        unimplemented!()
    }
}
