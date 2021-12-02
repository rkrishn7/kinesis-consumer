mod aws;
mod consumer;
mod proto;
mod storage;

use tonic::{transport::Server, Request, Response, Status};

use futures::stream::{self, StreamExt};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;

use storage::manager::SyncManager;
use storage::memory_manager::MemoryManager;

use proto::consumer_server::{Consumer, ConsumerServer};
use proto::{
    CheckpointConsumerRequest, CheckpointConsumerResponse, GetRecordsRequest, GetRecordsResponse,
    KdsRecord, ShutdownConsumerRequest, ShutdownConsumerResponse,
};

use consumer::lease::ConsumerLease;

use rusoto_core::Region;
use rusoto_kinesis::StreamDescription;
use rusoto_kinesis::{Kinesis, KinesisClient};

pub struct KinesisConsumerService<T>
where
    T: SyncManager,
{
    manager: Arc<Mutex<T>>,
    kinesis_client: KinesisClient,
}

impl<T> KinesisConsumerService<T>
where
    T: SyncManager,
{
    pub fn new(manager: Arc<Mutex<T>>, client: KinesisClient) -> Self {
        Self {
            manager,
            kinesis_client: client,
        }
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
        let (tx, rx) = mpsc::channel(100);

        let mut manager = self.manager.lock().unwrap();

        for lease in manager
            .available_leases_mut()
            .iter_mut()
            .filter(|lease| message.streams.contains(&lease.stream_name))
        {
            (*lease).claim();

            let inner_tx = tx.clone();
            let kinesis_client = self.kinesis_client.clone();
            let lease_clone = lease.clone();

            tokio::spawn(async move {
                let starting_position = rusoto_kinesis::StartingPosition {
                    sequence_number: lease_clone.last_processed_sn.clone(),
                    timestamp: None,
                    type_: lease_clone
                        .last_processed_sn
                        .map(|_| "AFTER_SEQUENCE_NUMBER".to_string())
                        .unwrap_or("TRIM_HORIZON".to_string()),
                };

                match kinesis_client
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
                                                    KdsRecord {
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
                .expect(format!("Unable to fetch stream description for {}", stream_name).as_str())
            });

        for stream_description in stream_descriptions_stream
            .collect::<Vec<StreamDescription>>()
            .await
        {
            let consumer_prefix = env!("CARGO_PKG_NAME");
            let consumer_name = format!("{}-{}", consumer_prefix, message.app_name);
            let stream_name = stream_description.stream_name;

            aws::kinesis::register_stream_consumer_if_not_exists(
                &self.kinesis_client,
                consumer_name,
                stream_description.stream_arn.to_owned(),
            )
            .await
            .expect(
                format!(
                    "Error registering stream consumer for stream {}",
                    stream_name
                )
                .as_str(),
            );

            tokio::time::sleep(Duration::from_secs(1)).await;

            for consumer in aws::kinesis::list_stream_consumers(
                &self.kinesis_client,
                stream_description.stream_arn,
            )
            .await
            .expect(format!("Unable to list stream consumers for stream {}", stream_name).as_str())
            .into_iter()
            .filter(|consumer| consumer.consumer_name.starts_with(consumer_prefix))
            {
                for shard_id in
                    aws::kinesis::get_shard_ids(&self.kinesis_client, stream_name.to_owned())
                        .await
                        .expect(
                            format!("Error getting shard ids for stream {}", stream_name).as_str(),
                        )
                {
                    let mut manager = self.manager.lock().unwrap();

                    let lease = ConsumerLease::new(
                        consumer.consumer_arn.to_owned(),
                        stream_name.to_owned(),
                        shard_id,
                    );

                    manager.create_lease_if_not_exists(lease);
                }
            }
        }

        let manager = self.manager.lock().unwrap();

        println!("available leases {:?}", manager.get_available_leases());

        Ok(Response::new(proto::InitializeResponse {
            successful: true,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let consumer = KinesisConsumerService::new(
        Arc::new(Mutex::new(MemoryManager::new())),
        KinesisClient::new(Region::default()),
    );

    Server::builder()
        .add_service(ConsumerServer::new(consumer))
        .serve(addr)
        .await?;

    Ok(())
}
