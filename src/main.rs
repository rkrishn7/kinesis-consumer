mod aws;
mod connection_manager;
mod consumer;
mod proto;
mod receiver_aware_stream;
mod storage;

use tonic::{transport::Server, Response, Status};

use futures::stream::{self, StreamExt};
use receiver_aware_stream::ReceiverAwareStream;
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

use connection_manager::{ConnectionManager, MemoryConnectionManager};

use lazy_static::lazy_static;

lazy_static! {
    static ref CONSUMER_ID: uuid::Uuid = uuid::Uuid::new_v4();
}

pub struct KinesisConsumerService<T, U: ConnectionManager> {
    storage_backend: T,
    kinesis_client: KinesisClient,
    connection_manager: U,
}

impl<T, U: ConnectionManager> KinesisConsumerService<T, U> {
    pub fn new(
        storage_backend: T,
        client: KinesisClient,
        connection_manager: U,
    ) -> Self {
        Self {
            storage_backend,
            kinesis_client: client,
            connection_manager,
        }
    }
}

#[tonic::async_trait]
impl<T, U> Consumer for KinesisConsumerService<T, U>
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
        let (record_sender, record_receiver) = mpsc::channel(100);
        let (stream_dropped_notifier, stream_dropped_receiver) =
            oneshot::channel::<()>();

        let streams = &message
            .streams
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&str>>();

        self.connection_manager
            .add_connection_for_streams(streams, remote_addr);

        let connection_manager = self.connection_manager.clone();

        let streams = message.streams.clone();

        let should_stop = stop.clone();

        // Spawn a task that waits until the `record_receiver` handle
        // has been dropped, which indicates the client has disconnected.
        // Subsequently, set `should_stop` to `true` and remove the connection
        // so we can kill dangling tasks and redistribute shards among the
        // active record processors.
        tokio::spawn(async move {
            let _ = stream_dropped_receiver.await;
            *should_stop.lock().unwrap() = true;
            let streams =
                &streams.iter().map(AsRef::as_ref).collect::<Vec<&str>>();
            connection_manager
                .remove_connection_for_streams(streams, remote_addr)
                .expect(
                    "Unable to remove connection. 
            This will result in an incorrect shard distribution across record 
            processors, which may result in shards being starved",
                );
        });

        let storage_backend = self.storage_backend.clone();
        let kinesis_client = self.kinesis_client.clone();
        let connection_manager = self.connection_manager.clone();
        let should_stop = stop;

        tokio::spawn(async move {
            loop {
                // When the client's connection drops, we must break so
                // we don't keep tasks unnecessarily running.
                if *should_stop.lock().unwrap() {
                    break;
                }

                let streams = &message
                    .streams
                    .iter()
                    .map(AsRef::as_ref)
                    .collect::<Vec<&str>>();

                // In order to evenly distribute shards to record processing clients,
                // we calculate a limit by dividing the total number of available leases
                // by the total number of connections, for `streams`. Only `limit` leases
                // are claimed by this task.
                let limit = {
                    let connection_count = connection_manager
                        .get_connection_count_for_streams(streams)
                        as i64;
                    let lease_count = storage_backend
                        .get_lease_count_for_streams(streams)
                        .await
                        .unwrap();

                    (lease_count + ((connection_count) - 1)) / connection_count
                };

                let claimed_leases = storage_backend
                    .claim_available_leases_for_streams(limit, streams)
                    .await
                    .unwrap();

                let mut task_handles = Vec::new();

                for lease in claimed_leases {
                    let record_sender = record_sender.clone();
                    let storage_backend = storage_backend.clone();
                    let kinesis_client = kinesis_client.clone();
                    let should_stop = should_stop.clone();

                    task_handles.push(tokio::task::spawn(async move {
                        let _ = tokio::time::timeout(
                            Duration::from_secs(20),
                            async {
                                let starting_position_type;
                                match &lease.last_processed_sn {
                                    Some(_) => starting_position_type = "AFTER_SEQUENCE_NUMBER".to_string(),
                                    None => starting_position_type = "TRIM_HORIZON".to_string(),
                                }

                                let mut event_stream = aws::kinesis::subscribe_to_shard(
                                    &kinesis_client,
                                    (&lease.shard_id).to_owned(),
                                    (&lease.consumer_arn).to_owned(),
                                    starting_position_type,
                                    None,
                                    lease.last_processed_sn.clone())
                                    .await.unwrap();

                                while let Some(item) = event_stream.next().await {
                                    // When the client's connection drops, we must break so
                                    // we don't keep tasks unnecessarily running.
                                    if *should_stop.lock().unwrap() {
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

                                                    match record_sender.send(Ok(GetRecordsResponse {
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

                        storage_backend
                            .release_lease(
                                &lease.consumer_arn,
                                &lease.stream_name,
                                &lease.shard_id,
                            )
                            .await
                            .expect(format!("Unable to release the following lease: {}. This may result in shards being starved.", lease).as_str());
                    }));
                }

                if task_handles.len() == 0 {
                    // Sleep the current task to avoid eating up CPU
                    // while there is no work to be done.
                    tokio::time::sleep(Duration::from_secs(5)).await;
                } else {
                    futures::future::join_all(task_handles).await;
                }
            }
        });

        Ok(Response::new(ReceiverAwareStream::new(
            record_receiver,
            Some(stream_dropped_notifier),
        )))
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

    // Postgres storage backend initialization
    // TODO: add check when more storage backends are added
    let mut postgres_storage_backend = PostgresKinesisStorageBackend::new(
        &*CONSUMER_ID,
        std::env::var("POSTGRES_DATABASE_URL").expect(
            "
        Expected environment variable `POSTGRES_DATABASE_URL` 
        when using value postgres as a storage backend",
        ),
    );
    postgres_storage_backend
        .init()
        .await
        .expect("Unable to initialize postgres storage backend");
    let kinesis_client = aws::kinesis::create_client();
    let connection_manager = MemoryConnectionManager::new();
    let consumer = KinesisConsumerService::new(
        postgres_storage_backend,
        kinesis_client,
        connection_manager,
    );

    Server::builder()
        .add_service(ConsumerServer::new(consumer))
        .serve(addr)
        .await?;

    Ok(())
}
