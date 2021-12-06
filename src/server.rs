mod aws;
mod consumer;
mod proto;
mod receiver_stream;
mod storage;

use tonic::{transport::Server, Request, Response, Status};

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

use rusoto_core::Region;
use rusoto_kinesis::KinesisClient;
use rusoto_kinesis::StreamDescription;

pub struct AsyncKinesisConsumerService<
    T: AsyncKinesisStorageBackend + Send + Sync + Clone,
> {
    storage_backend: T,
    kinesis_client: KinesisClient,
    streamers: Arc<Mutex<usize>>,
}

impl<T: AsyncKinesisStorageBackend + Send + Sync + Clone>
    AsyncKinesisConsumerService<T>
{
    pub fn new(storage_backend: T, client: KinesisClient) -> Self {
        Self {
            storage_backend,
            kinesis_client: client,
            streamers: Arc::new(Mutex::new(0)),
        }
    }
}

#[tonic::async_trait]
impl<T> Consumer for AsyncKinesisConsumerService<T>
where
    T: AsyncKinesisStorageBackend + Send + Sync + Clone + 'static,
{
    type GetRecordsStream =
        ReceiverAwareStream<Result<GetRecordsResponse, Status>>;

    async fn get_records(
        &self,
        request: tonic::Request<GetRecordsRequest>,
    ) -> Result<tonic::Response<Self::GetRecordsStream>, tonic::Status> {
        let message = request.into_inner();
        let stop = Arc::new(Mutex::new(false));
        let (tx, rx) = mpsc::channel(100);

        let (one_tx, one_rx) = oneshot::channel::<()>();

        {
            let mut streamers = self.streamers.lock().unwrap();
            *streamers += 1;
        }

        let updater = self.streamers.clone();
        let stop_clone = stop.clone();

        tokio::spawn(async move {
            match one_rx.await {
                Ok(_) => {
                    let mut streamers = updater.lock().unwrap();
                    let mut stop = stop_clone.lock().unwrap();
                    *stop = true;
                    *streamers -= 1;
                    println!("active streamers: {}", streamers);
                }
                Err(e) => {
                    println!("Error receiving receiver stream notify: {}", e)
                }
            }
        });

        let storage_backend = self.storage_backend.clone();
        let kinesis_client = self.kinesis_client.clone();
        let streamers = self.streamers.clone();

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

                let mut futs = Vec::new();

                {
                    let max = 3; // TODO: update
                    let claimed_leases = storage_backend
                        .claim_available_leases_for_streams(
                            max,
                            &message
                                .streams
                                .iter()
                                .map(AsRef::as_ref)
                                .collect(),
                        )
                        .await
                        .unwrap();

                    for lease in claimed_leases {
                        let tx = tx.clone();
                        let storage_backend = storage_backend.clone();
                        let kinesis_client = kinesis_client.clone();
                        let stop = stop.clone();

                        futs.push(async move {
                            let lease = lease.clone();
                            let lease_clone_2 = lease.clone();
                            let _ = tokio::time::timeout(Duration::from_secs(20), async move {
                                let starting_position_type = lease.last_processed_sn.as_ref()
                                .map(|_| "AFTER_SEQUENCE_NUMBER".to_string())
                                .unwrap_or("TRIM_HORIZON".to_string());

                                let mut event_stream = aws::kinesis::subscribe_to_shard(&kinesis_client, lease.shard_id.to_owned(), lease.consumer_arn.to_owned(), starting_position_type.clone(), None, lease.last_processed_sn.clone()).await.unwrap();

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
                            }).await;

                            println!("Releasing shard lease for lease {:?}", lease_clone_2);
                            let _ = storage_backend.release_lease(&lease_clone_2.consumer_arn, &lease_clone_2.stream_name, &lease_clone_2.shard_id).await.unwrap();
                        });
                    }
                }

                if futs.len() == 0 {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                } else {
                    futures::future::join_all(futs).await;
                }
            }

            println!("broke outer loop");
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
    let consumer = AsyncKinesisConsumerService::new(
        storage_backend,
        KinesisClient::new(Region::default()),
    );

    Server::builder()
        .add_service(ConsumerServer::new(consumer))
        .serve(addr)
        .await?;

    Ok(())
}
