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
    streamers: Arc<Mutex<usize>>,
}

impl<T> KinesisConsumerService<T>
where
    T: SyncManager,
{
    pub fn new(manager: Arc<Mutex<T>>, client: KinesisClient) -> Self {
        Self {
            manager,
            kinesis_client: client,
            streamers: Arc::new(Mutex::new(0)),
        }
    }
}

#[tonic::async_trait]
impl<T> Consumer for KinesisConsumerService<T>
where
    T: SyncManager + Send + Sync + 'static,
{
    type GetRecordsStream = ReceiverAwareStream<Result<GetRecordsResponse, Status>>;

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
                Err(e) => println!("Error receiving receiver stream notify: {}", e),
            }
        });

        let manager_guard = self.manager.clone();
        let kinesis_client = self.kinesis_client.clone();
        let streamers_clone = self.streamers.clone();

        let stop = stop.clone();

        tokio::spawn(async move {
            loop {
                let should_stop = {
                    let should_stop = stop.lock().unwrap();
                    *should_stop
                };
                
                if should_stop { break; }
                
                let mut futs = Vec::new();
                {
                    let mut manager = manager_guard.lock().unwrap();
                    let streamers_count = streamers_clone.lock().unwrap();

                    let mut available_leases = manager
                        .available_leases_mut()
                        .into_iter()
                        .filter(|lease| message.streams.contains(&lease.stream_name))
                        .collect::<Vec<&mut ConsumerLease>>();

                    let mut i = 0;
                    let max = (available_leases.len() as f32 / *streamers_count as f32).ceil() as usize;

                    while i < max
                    {
                        println!("max {}", max);
                        (available_leases[i]).claim();
                        let lease = available_leases[i].clone();

                        let inner_tx = tx.clone();

                        let lease_clone = lease.clone();
                        let manager_clone = manager_guard.clone();
                        let kinesis_client = kinesis_client.clone();

                        let stop = stop.clone();

                        futs.push(async move {
                            let lease_clone_2 = lease_clone.clone();
                            let _ = tokio::time::timeout(Duration::from_secs(20), async move {
                                let starting_position = rusoto_kinesis::StartingPosition {
                                    sequence_number: lease_clone.last_processed_sn.clone(),
                                    timestamp: None,
                                    type_: lease_clone
                                        .last_processed_sn
                                        .clone()
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
                                            let _should_stop = {
                                                let should_stop = stop.lock().unwrap();
                                                *should_stop
                                            };

                                            println!("_should stop {}", _should_stop);

                                            if _should_stop {
                                                println!("stopping event stream reading");
                                                break;
                                            }

                                            match item {
                                                Ok(payload) => {
                                                    println!(
                                                        "Got event from the event stream: {:?}",
                                                        payload
                                                    );
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
        
                                                            match inner_tx.send(Ok(GetRecordsResponse {
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
                                    }
                                    Err(_) => todo!(),
                                }
                            }).await;
                            {
                                println!("Releasing shard lease for lease {:?}", lease_clone_2);
                                let mut lock = manager_clone.lock().unwrap();
                                lock.release_lease(lease);
                                println!(
                                    "Shard lease released. available leases: {:?}",
                                    lock.get_available_leases()
                                );
                            }
                        });

                        i += 1;
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
                .map_err(|_| tonic::Status::new(tonic::Code::Internal, "Error describing stream"))
            });

        // Traverse through each stream in `message.streams`
        for stream_description_result in stream_descriptions_stream
            .collect::<Vec<Result<StreamDescription, tonic::Status>>>()
            .await
        {
            let stream_description = stream_description_result?;

            let consumer_prefix = env!("CARGO_PKG_NAME");
            let consumer_name = format!("{}-{}", consumer_prefix, message.app_name);
            let stream_name = stream_description.stream_name;

            aws::kinesis::register_stream_consumer_if_not_exists(
                &self.kinesis_client,
                consumer_name,
                stream_description.stream_arn.to_owned(),
            )
            .await
            .map_err(|_| {
                tonic::Status::new(tonic::Code::Internal, "Error registering stream consumer")
            })?;

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
