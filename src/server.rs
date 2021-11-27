mod consumer;
mod proto;
mod storage;

use tonic::{transport::Server, Request, Response, Status};

use futures_util::stream::StreamExt;

use futures_core::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
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
use rusoto_kinesis::ListShardsInput;
use rusoto_kinesis::{Kinesis, KinesisClient};

#[derive(Debug, Default)]
pub struct KinesisConsumerService {}

#[tonic::async_trait]
impl Consumer for KinesisConsumerService {
    type GetRecordsStream = ReceiverStream<Result<GetRecordsResponse, Status>>;

    async fn get_records(
        &self,
        request: tonic::Request<GetRecordsRequest>,
    ) -> Result<tonic::Response<Self::GetRecordsStream>, tonic::Status> {
        let message = request.into_inner();
        let (mut tx, rx) = mpsc::channel(100);

        let consumers = message.consumers;
        let mut manager = MemoryManager::new();
        let client = KinesisClient::new(Region::Custom {
            name: "us-west-2".to_owned(),
            endpoint: "http://localhost:4566".to_owned(),
        });

        for consumer in consumers.into_iter() {
            let consumer_stream = consumer.stream_name.clone();

            match client
                .list_shards(ListShardsInput {
                    stream_name: Some(consumer_stream.clone()),
                    ..Default::default()
                })
                .await
            {
                Ok(output) => match output.shards {
                    Some(shards) => {
                        for shard in shards.into_iter() {
                            let lease = ConsumerLease::new(
                                consumer.arn.to_owned(),
                                consumer.stream_name.to_owned(),
                                shard.shard_id,
                            );

                            manager.create_lease_if_not_exists(lease);
                        }
                    }
                    None => {}
                },
                Err(_) => todo!(),
            }

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
                                                            encryption_type: record.encryption_type.unwrap().to_string(),
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
                                    },
                                }
                            }
                        }
                        Err(_) => todo!(),
                    }
                });
            }
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
}

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
