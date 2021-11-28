mod consumer;
mod proto;
mod storage;

use tonic::{transport::Server, Request, Response, Status};

use futures::stream::{self, StreamExt};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use std::sync::{Arc};
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;

use storage::manager::SyncManager;
use storage::memory_manager::MemoryManager;

use proto::consumer_server::{Consumer, ConsumerServer};
use proto::{
    kds_record::KdsRecordMetadata, CheckpointConsumerRequest, CheckpointConsumerResponse,
    GetRecordsRequest, GetRecordsResponse, KdsRecord, ShutdownConsumerRequest,
    ShutdownConsumerResponse,
};

use consumer::lease::ConsumerLease;

use rusoto_core::{Region, RusotoError};
use rusoto_kinesis::{DescribeStreamInput, DescribeStreamOutput, ListShardsInput, StreamDescription, RegisterStreamConsumerInput};
use rusoto_kinesis::{Kinesis, KinesisClient};

#[derive(Debug, Default)]
pub struct KinesisConsumerService<T>
where
    T: SyncManager,
{
    manager: Arc<Mutex<T>>,
}

impl<T> KinesisConsumerService<T>
where
    T: SyncManager,
{
    pub fn new(manager: Arc<Mutex<T>>) -> Self {
        Self { manager }
    }
}

#[tonic::async_trait]
impl<T> Consumer for KinesisConsumerService<T>
where
    T: SyncManager + Send + Sync + 'static,
{
    type GetRecordsStream = ReceiverStream<Result<GetRecordsResponse, Status>>;

    async fn get_records(
        &self,
        request: tonic::Request<GetRecordsRequest>,
    ) -> Result<tonic::Response<Self::GetRecordsStream>, tonic::Status> {
        let message = request.into_inner();
        let (mut tx, rx) = mpsc::channel(100);

        let mut manager = self.manager.lock().await;
        let client = KinesisClient::new(Region::default());

        let available_leases = manager.get_available_leases();

        for &lease in available_leases
            .iter()
            .filter(move |&&lease| lease.stream_name == consumer_stream)
        {
            let inner_tx = tx.clone();
            let cloned_client = client.clone();
            let lease_clone = lease.clone();
            let consumer_clone = consumer.clone();

            tokio::spawn(async move {
                let starting_position: rusoto_kinesis::StartingPosition;

                if let Some(sequence_number) = lease_clone.last_processed_sn {
                    starting_position = rusoto_kinesis::StartingPosition {
                        sequence_number: Some(sequence_number.to_owned()),
                        timestamp: None,
                        type_: String::from("AFTER_SEQUENCE_NUMBER"),
                    };
                } else {
                    starting_position = rusoto_kinesis::StartingPosition {
                        sequence_number: None,
                        timestamp: None,
                        type_: String::from("TRIM_HORIZON"),
                    };
                }

                match cloned_client
                    .subscribe_to_shard(rusoto_kinesis::SubscribeToShardInput {
                        consumer_arn: lease_clone.consumer_arn.to_owned(),
                        shard_id: lease_clone.shard_id.to_owned(),
                        starting_position,
                    })
                    .await
                {
                    Ok(output) => {
                        let mut event_stream = output.event_stream;

                        while let Some(item) = event_stream.next().await {
                            match item {
                                Ok(payload) => {
                                    println!("Got event from the event stream: {:?}", payload);
                                    println!("shard id: {}", lease_clone.shard_id);

                                    match payload {
                                        rusoto_kinesis::SubscribeToShardEventStreamItem::SubscribeToShardEvent(e) => {
                                            let mut send_records: Vec<KdsRecord> = Vec::new();

                                            send_records.extend(
                                                e.records.into_iter().map(|record| {
                                                    let record_metadata = KdsRecordMetadata {
                                                        consumer: Some(consumer_clone.clone()),
                                                    };
                                                    
                                                    KdsRecord {
                                                        metadata: Some(record_metadata),
                                                        sequence_number: record.sequence_number,
                                                        timestamp: record.approximate_arrival_timestamp.unwrap().to_string(),
                                                        partition_key: record.partition_key,
                                                        encryption_type: record.encryption_type.unwrap_or(String::from("")).to_string(),
                                                        data: record.data.into_iter().collect(),
                                                    }
                                                })
                                            );

                                            inner_tx.send(Ok(GetRecordsResponse {
                                                records: send_records,
                                            })).await.unwrap();
                                        },
                                        _ => todo!(),
                                    }
                                }
                                Err(e) => {
                                    println!("{:?}", e);
                                }
                            }
                        }
                    }
                    Err(_) => todo!(),
                }
            });
        }

        Ok(Response::new(ReceiverStream::new(rx)))
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

    async fn initialize(&self, request:tonic::Request<proto::InitializeRequest>) -> Result<tonic::Response<proto::InitializeResponse>,tonic::Status> {
        let message = request.into_inner();

        let client = KinesisClient::new(Region::default());

        let stream = stream::iter(message.streams).then(|stream_name| async {

            match client.describe_stream(DescribeStreamInput {
                exclusive_start_shard_id: None,
                limit: None,
                stream_name,
            }).await {
                Ok(describe_stream_output) => {
                    describe_stream_output.stream_description
                },
                Err(rusoto_error) => {
                    // TODO: Better error handling
                    match rusoto_error {
                        RusotoError::Service(describe_stream_error) => {
                            match describe_stream_error {
                                rusoto_kinesis::DescribeStreamError::LimitExceeded(message) => {
                                    println!("{}", message);
                                    panic!();
                                },
                                rusoto_kinesis::DescribeStreamError::ResourceNotFound(message) => {
                                    println!("{}", message);
                                    panic!();
                                },
                            }
                        },
                        RusotoError::HttpDispatch(_) => todo!(),
                        RusotoError::Credentials(_) => todo!(),
                        RusotoError::Validation(_) => todo!(),
                        RusotoError::ParseError(_) => todo!(),
                        RusotoError::Unknown(_) => todo!(),
                        RusotoError::Blocking => todo!(),
                    }
                },
            }
        });

        for stream_description in stream.collect::<Vec<StreamDescription>>().await {
            let consumer_prefix = "jarvis";

            let consumer = async {
                match client.register_stream_consumer(RegisterStreamConsumerInput {
                    consumer_name: format!("{}-{}", consumer_prefix, message.app_name),
                    stream_arn: stream_description.stream_arn.to_owned(),
                }).await {
                    Ok(register_stream_consumer_output) => {
                        register_stream_consumer_output.consumer
                    },
                    Err(e) => {
                        println!("{:?}", e);

                        panic!();
                    },
                }
            }.await;

            let mut manager = self.manager.lock().await;

            for shard in stream_description.shards {
                let lease = ConsumerLease::new(
                    consumer.consumer_arn.to_owned(),
                    stream_description.stream_name.to_owned(),
                    shard.shard_id,
                );

                manager.create_lease_if_not_exists(lease);
            }

            println!("available leases {:?}", manager.get_available_leases());
        }

        Ok(Response::new(proto::InitializeResponse {
            successful: true,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let consumer = KinesisConsumerService::new(Arc::new(Mutex::new(MemoryManager::new())));

    Server::builder()
        .add_service(ConsumerServer::new(consumer))
        .serve(addr)
        .await?;

    Ok(())
}
