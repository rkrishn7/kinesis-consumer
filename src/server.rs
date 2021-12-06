mod aws;
mod consumer;
mod proto;
mod receiver_stream;
mod storage;
mod connection_manager;

use tonic::{transport::Server, Response, Status};

use futures::stream::{self, StreamExt};
use receiver_stream::ReceiverAwareStream;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::Duration;

use storage::postgres::PostgresKinesisStorageBackend;
use storage::AsyncKinesisStorageBackend;

use proto::consumer_server::{Consumer, ConsumerServer};
use proto::{
    CheckpointConsumerRequest, CheckpointConsumerResponse, GetRecordsRequest,
    GetRecordsResponse, KdsRecord, ShutdownConsumerRequest,
    ShutdownConsumerResponse,
};

use rusoto_kinesis::KinesisClient;
use rusoto_kinesis::StreamDescription;

use connection_manager::{MemoryConnectionManager, ConnectionManager};

pub struct AsyncKinesisConsumerService<T, U: ConnectionManager> {
    storage_backend: T,
    kinesis_client: KinesisClient,
    connection_manager: U,
}

impl<T, U: ConnectionManager> AsyncKinesisConsumerService<T, U> {
    pub fn new(storage_backend: T, client: KinesisClient, connection_manager: U) -> Self {
        Self {
            storage_backend,
            kinesis_client: client,
            connection_manager,
        }
    }
}

#[tonic::async_trait]
impl<T, U> Consumer for AsyncKinesisConsumerService<T, U>
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
        let stop = Arc::new(Mutex::new(false));
        let (tx, rx) = mpsc::channel(100);

        let (one_tx, one_rx) = oneshot::channel::<()>();

        let streams = &message
        .streams
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<&str>>();

        self.connection_manager.add_connection_for_streams(streams, remote_addr);

        println!("Connections {:?}", self.connection_manager.get_connections_for_streams(streams));

        let connection_manager = self.connection_manager.clone();
        let stop_clone = stop.clone();

        let streams = message.streams.clone();

        tokio::spawn(async move {
            match one_rx.await {
                Ok(_) => {
                    let streams = &streams
                    .iter()
                    .map(AsRef::as_ref)
                    .collect::<Vec<&str>>();
                    let _ = connection_manager.remove_connection_for_streams(streams, remote_addr);
                    println!("Connections {:?}", connection_manager.get_connections_for_streams(streams));
                    let mut stop = stop_clone.lock().unwrap();
                    *stop = true;
                }
                Err(e) => {
                    println!("Error receiving receiver stream notify: {}", e)
                }
            }
        });

        let storage_backend = self.storage_backend.clone();
        let kinesis_client = self.kinesis_client.clone();
        let connection_manager = self.connection_manager.clone();

        tokio::spawn(async move {
            loop {
                // We need to reassign leases if one of the following occur:
                // 1. A new connection starts attempting to stream records
                //    from a set of streams that intersects with the streams
                //    in the parent task's `request.message.streams`.
                // 2. A connection that was reading from a set of streams that
                //    intersects with the parent task's set of streams has dropped.
                if *stop.lock().unwrap() {
                    break;
                }

                let streams = &message
                    .streams
                    .iter()
                    .map(AsRef::as_ref)
                    .collect::<Vec<&str>>();

                let limit = {
                    let connection_count = connection_manager.get_connection_count_for_streams(streams) as i64;
                    let lease_count = storage_backend
                        .get_lease_count_for_streams(streams)
                        .await
                        .unwrap();

                    (lease_count + ((connection_count) - 1))
                        / connection_count
                };

                let claimed_leases = storage_backend
                    .claim_available_leases_for_streams(limit, streams)
                    .await
                    .unwrap();

                let mut task_handles = Vec::new();

                for lease in claimed_leases {
                    let tx = tx.clone();
                    let storage_backend = storage_backend.clone();
                    let kinesis_client = kinesis_client.clone();
                    let stop = stop.clone();

                    task_handles.push(tokio::task::spawn(async move {
                        let lease_clone = lease.clone();
                        let _ = tokio::time::timeout(
                            Duration::from_secs(20),
                            async move {
                                let starting_position_type;
                                match &lease.last_processed_sn {
                                    Some(_) => starting_position_type = "AFTER_SEQUENCE_NUMBER".to_string(),
                                    None => starting_position_type = "TRIM_HORIZON".to_string(),
                                }

                                let mut event_stream = aws::kinesis::subscribe_to_shard(&kinesis_client, (&lease.shard_id).to_owned(), (&lease.consumer_arn).to_owned(), starting_position_type, None, lease.last_processed_sn.clone()).await.unwrap();

                                while let Some(item) = event_stream.next().await {
                                    if *stop.lock().unwrap() {
                                        break;
                                    }

                                    match item {
                                        Ok(payload) => {
                                            println!(
                                                "Got event from the event stream: {:?}",
                                                payload
                                            );
                                            println!("shard id: {}", lease.shard_id);

                                            match payload {
                                                rusoto_kinesis::SubscribeToShardEventStreamItem::SubscribeToShardEvent(e) => {
                                                    let mut send_records: Vec<KdsRecord> = Vec::new();

                                                    send_records.extend(
                                                        e.records.into_iter().map(|record| {
                                                            KdsRecord {
                                                                sequence_number: record.sequence_number,
                                                                timestamp: record.approximate_arrival_timestamp.unwrap().to_string(),
                                                                partition_key: record.partition_key,
                                                                encryption_type: record.encryption_type.unwrap_or(String::from("")).to_string(),
                                                                data: record.data.into_iter().collect(),
                                                            }
                                                        })
                                                    );

                                                    match tx.send(Ok(GetRecordsResponse {
                                                        records: send_records,
                                                    })).await {
                                                        Ok(_) => (),
                                                        Err(e) => {
                                                            println!("{}", e);
                                                            break
                                                        },
                                                    }
                                                },
                                                _ => todo!(),
                                            }
                                        }
                                        Err(e) => {
                                            println!("{:?}", e);
                                        }
                                    }
                                }
                            },
                        )
                        .await;
                        
                        println!(
                            "Releasing shard lease for lease {:?}",
                            lease_clone
                        );
                        let _ = storage_backend
                            .release_lease(
                                &lease_clone.consumer_arn,
                                &lease_clone.stream_name,
                                &lease_clone.shard_id,
                            )
                            .await
                            .unwrap();
                    }));
                }

                if task_handles.len() == 0 {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                } else {
                    futures::future::join_all(task_handles).await;
                }
            }
        });

        Ok(Response::new(ReceiverAwareStream::new(rx, Some(one_tx))))
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
    ) -> Result<tonic::Response<CheckpointConsumerResponse>, tonic::Status>
    {
        unimplemented!()
    }

    async fn initialize(
        &self,
        request: tonic::Request<proto::InitializeRequest>,
    ) -> Result<tonic::Response<proto::InitializeResponse>, tonic::Status> {
        let message = request.into_inner();

        let stream_descriptions_stream =
            stream::iter(message.streams).then(|stream_name| async move {
                aws::kinesis::describe_stream(
                    &self.kinesis_client,
                    stream_name.to_owned(),
                    None,
                    None,
                )
                .await
                .map_err(|_| {
                    tonic::Status::new(
                        tonic::Code::Internal,
                        "Error describing stream",
                    )
                })
            });

        // Traverse through each stream in `message.streams`
        for stream_description_result in stream_descriptions_stream
            .collect::<Vec<Result<StreamDescription, tonic::Status>>>()
            .await
        {
            let stream_description = stream_description_result?;

            let consumer_prefix = env!("CARGO_PKG_NAME");
            let consumer_name =
                format!("{}-{}", consumer_prefix, message.app_name);
            let stream_name = stream_description.stream_name;

            aws::kinesis::register_stream_consumer_if_not_exists(
                &self.kinesis_client,
                consumer_name,
                stream_description.stream_arn.to_owned(),
            )
            .await
            .map_err(|_| {
                tonic::Status::new(
                    tonic::Code::Internal,
                    "Error registering stream consumer",
                )
            })?;

            for consumer in aws::kinesis::list_stream_consumers(
                &self.kinesis_client,
                stream_description.stream_arn,
            )
            .await
            .expect(
                format!(
                    "Unable to list stream consumers for stream {}",
                    stream_name
                )
                .as_str(),
            )
            .into_iter()
            .filter(|consumer| {
                consumer.consumer_name.starts_with(consumer_prefix)
            }) {
                for shard_id in aws::kinesis::get_shard_ids(
                    &self.kinesis_client,
                    stream_name.to_owned(),
                )
                .await
                .expect(
                    format!(
                        "Error getting shard ids for stream {}",
                        stream_name
                    )
                    .as_str(),
                ) {
                    let _ = self
                        .storage_backend
                        .create_lease_if_not_exists(
                            &consumer.consumer_arn,
                            &stream_name,
                            &shard_id,
                        )
                        .await
                        .unwrap();
                }
            }
        }

        Ok(Response::new(proto::InitializeResponse {
            successful: true,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let storage_backend = PostgresKinesisStorageBackend::init().await;
    let kinesis_client = aws::kinesis::create_client();
    let connection_manager = MemoryConnectionManager::new();
    let consumer =
        AsyncKinesisConsumerService::new(storage_backend, kinesis_client, connection_manager);

    Server::builder()
        .add_service(ConsumerServer::new(consumer))
        .serve(addr)
        .await?;

    Ok(())
}
