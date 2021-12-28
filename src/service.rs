use std::time::Duration;

use crate::aws::kinesis;
use crate::connection_manager::ConnectionManager;
use crate::consumer::lease::ConsumerLease;
use crate::proto::DataRecord;
use crate::proto::DataRecords;
use crate::proto::Lease;
use crate::storage::AsyncKinesisStorageBackend;
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::Future;
use futures::StreamExt;
use rusoto_kinesis::KinesisClient;
use rusoto_kinesis::SubscribeToShardEventStreamItem;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct KinesisConsumerService<T: Clone, U: Clone> {
    pub storage_backend: T,
    pub kinesis_client: KinesisClient,
    pub connection_manager: U,
}

impl<T: Clone, U: Clone> KinesisConsumerService<T, U> {
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

    #[inline(always)]
    fn get_consumer_name(app_name: &str) -> String {
        let consumer_prefix = env!("CARGO_PKG_NAME");
        format!("{}-{}", consumer_prefix, app_name)
    }
}

impl<
        T: AsyncKinesisStorageBackend + Clone + Send + Sync + 'static,
        U: ConnectionManager + Clone + Send + Sync + 'static,
    > KinesisConsumerService<T, U>
{
    pub async fn refresh_consumer_leases(
        &self,
        streams: Vec<String>,
        app_name: String,
    ) -> Result<(), anyhow::Error> {
        // Run tasks in chunks so we don't exceed limits
        for streams in &mut streams[..].chunks(
            kinesis::MAX_REGISTER_STREAM_CONSUMER_TRANSACTIONS_PER_SECOND,
        ) {
            let handles = streams.into_iter().map(|stream_name| {
                let kinesis_client = self.kinesis_client.clone();
                let storage_backend = self.storage_backend.clone();
                let app_name = app_name.clone();
                let stream_name = stream_name.to_owned();

                tokio::spawn(async move {
                    let stream_description = kinesis::describe_stream(
                        &kinesis_client,
                        stream_name.to_owned(),
                        None,
                        None,
                    )
                    .await?;

                    let consumer = kinesis::get_or_register_stream_consumer(
                        &kinesis_client,
                        Self::get_consumer_name(app_name.as_str()),
                        stream_description.stream_arn.to_owned(),
                    )
                    .await?;

                    for shard_id in kinesis::get_shard_ids(
                        &kinesis_client,
                        stream_name.to_owned(),
                    )
                    .await?
                    {
                        storage_backend
                            .create_lease_if_not_exists(
                                &consumer.consumer_arn,
                                &stream_name,
                                &shard_id,
                                app_name.as_str(),
                            )
                            .await
                            .unwrap();
                    }

                    Ok::<(), anyhow::Error>(())
                })
            });

            try_join_all(handles).await?;
        }

        Ok(())
    }

    pub async fn serve<G>(
        &self,
        app_name: String,
        streams: Vec<String>,
        tx: Sender<G>,
    ) where
        G: From<DataRecords> + Send + 'static,
    {
        loop {
            let connection_count = self
                .connection_manager
                .get_connection_count_for_streams(&streams)
                as i64;

            // In order to evenly distribute shards to record processing clients,
            // we calculate a limit by dividing the total number of available leases
            // by the total number of connections, for `streams`. Only `limit` leases
            // are claimed by this task.
            let limit = {
                let lease_count = self
                    .storage_backend
                    .get_lease_count_for_streams(&streams, app_name.as_str())
                    .await
                    .unwrap();

                (lease_count + ((connection_count) - 1)) / connection_count
            };

            let claimed_leases = self
                .storage_backend
                .claim_available_leases_for_streams(
                    limit,
                    &streams,
                    app_name.as_str(),
                )
                .await
                .unwrap();

            if claimed_leases.len() == 0 {
                tokio::time::sleep(Duration::from_secs(5)).await;
            } else {
                let mut consumer_tasks =
                    self.consume_multiple(claimed_leases, tx.clone());

                while let Some(lease) = consumer_tasks.next().await {
                    println!("releasing lease");
                    let lease = lease.unwrap();

                    self.storage_backend
                    .release_lease(
                        &lease.consumer_arn,
                        &lease.stream_name,
                        &lease.shard_id,
                        app_name.as_str(),
                    )
                    .await
                    .expect(format!("Unable to release lease: {}. This may result in shards being starved.", lease).as_str());
                }

                println!("howdy");
            }
        }
    }
}

impl<T: AsyncKinesisStorageBackend + Clone, U: Clone>
    KinesisConsumerService<T, U>
{
    /// Consumes records from the Kinesis shard owned by the specified lease.
    /// Produces sets of data records and sends them on a `mpsc::Channel`.
    pub fn consume<G>(
        &self,
        lease: ConsumerLease,
        tx: Sender<G>,
    ) -> impl Future<Output = ConsumerLease>
    where
        G: From<DataRecords>,
    {
        let kinesis_client = self.kinesis_client.clone();

        async move {
            let starting_position_type = lease.get_starting_position_type();

            let mut event_stream = kinesis::subscribe_to_shard(
                &kinesis_client,
                (&lease.shard_id).to_owned(),
                (&lease.consumer_arn).to_owned(),
                starting_position_type.to_string(),
                None,
                lease.last_processed_sn.clone(),
            )
            .await
            .unwrap();

            while let Some(item) = event_stream.next().await {
                match item {
                    Ok(
                        SubscribeToShardEventStreamItem::SubscribeToShardEvent(
                            event,
                        ),
                    ) => {
                        let records = DataRecords(
                            event
                                .records
                                .into_iter()
                                .map(|record| DataRecord {
                                    sequence_number: record.sequence_number,
                                    timestamp: record
                                        .approximate_arrival_timestamp
                                        .unwrap()
                                        .to_string(),
                                    partition_key: record.partition_key,
                                    encryption_type: record
                                        .encryption_type
                                        .unwrap_or(String::from(""))
                                        .to_string(),
                                    data: record.data.into_iter().collect(),
                                    lease: Some(Lease {
                                        stream_name: lease
                                            .stream_name
                                            .to_owned(),
                                        shard_id: lease.shard_id.to_owned(),
                                        consumer_arn: lease
                                            .consumer_arn
                                            .to_owned(),
                                    }),
                                })
                                .collect(),
                        );

                        if let Err(_) = tx.send(records.into()).await {
                            println!("helo");
                            break;
                        }
                    }
                    Ok(event_stream_error) => {
                        // TODO: handle
                        break;
                    }
                    Err(rusoto_error) => {
                        // TODO: handle
                        break;
                    }
                }
            }

            println!("gi");
            lease
        }
    }
}

impl<T: AsyncKinesisStorageBackend + Clone + 'static, U: Clone + 'static>
    KinesisConsumerService<T, U>
{
    pub fn consume_multiple<G>(
        &self,
        leases: Vec<ConsumerLease>,
        tx: Sender<G>,
    ) -> FuturesUnordered<JoinHandle<ConsumerLease>>
    where
        G: From<DataRecords> + Send + 'static,
    {
        leases
            .into_iter()
            .map(|lease| tokio::spawn(self.consume(lease, tx.clone())))
            .collect::<FuturesUnordered<JoinHandle<ConsumerLease>>>()
    }
}

// tokio::spawn(async move {
//     loop {
//         let app_name = app_name.clone();
//         let connection_count = connection_manager
//             .get_connection_count_for_streams(&streams)
//             as i64;

//         // In order to evenly distribute shards to record processing clients,
//         // we calculate a limit by dividing the total number of available leases
//         // by the total number of connections, for `streams`. Only `limit` leases
//         // are claimed by this task.
//         let limit = {
//             let lease_count = storage_backend
//                 .get_lease_count_for_streams(
//                     &streams,
//                     app_name.as_str(),
//                 )
//                 .await
//                 .unwrap();

//             (lease_count + ((connection_count) - 1)) / connection_count
//         };

//         println!("limit {}", limit);

//         let claimed_leases = storage_backend
//             .claim_available_leases_for_streams(
//                 limit,
//                 &streams,
//                 app_name.as_str(),
//             )
//             .await
//             .unwrap();

//         let mut task_handles = Vec::new();

//         for lease in claimed_leases {
//             let record_sender = record_sender.clone();
//             let storage_backend = storage_backend.clone();
//             let kinesis_client = kinesis_client.clone();
//             let connection_manager = connection_manager.clone();
//             let streams = streams.clone();
//             let app_name = app_name.clone();

//             task_handles.push(tokio::task::spawn(async move {
//                   let starting_position_type;
//                   match &lease.last_processed_sn {
//                       Some(_) => starting_position_type = "AFTER_SEQUENCE_NUMBER".to_string(),
//                       None => starting_position_type = "TRIM_HORIZON".to_string(),
//                   }

//                   let mut event_stream = kinesis::subscribe_to_shard(
//                       &kinesis_client,
//                       (&lease.shard_id).to_owned(),
//                       (&lease.consumer_arn).to_owned(),
//                       starting_position_type,
//                       None,
//                       lease.last_processed_sn.clone())
//                       .await.unwrap();

//                   while let Some(item) = event_stream.next().await {
//                       // When the client's connection drops, we must break so
//                       // we don't keep tasks unnecessarily running.
//                       if connection_count != (connection_manager.get_connection_count_for_streams(&streams) as i64) {
//                           println!("connection imbalance");
//                           break;
//                       }

//                       match item {
//                           Ok(payload) => {
//                               println!(
//                                   "Got event from the event stream: {:?}",
//                                   payload
//                               );
//                               println!("shard id: {}", lease.shard_id);

//                               match payload {
//                                   rusoto_kinesis::SubscribeToShardEventStreamItem::SubscribeToShardEvent(e) => {
//                                       let mut send_records: Vec<DataRecord> = Vec::new();

//                                       send_records.extend(
//                                           e.records.into_iter().map(|record| {
//                                               DataRecord {
//                                                   sequence_number: record.sequence_number,
//                                                   timestamp: record.approximate_arrival_timestamp.unwrap().to_string(),
//                                                   partition_key: record.partition_key,
//                                                   encryption_type: record.encryption_type.unwrap_or(String::from("")).to_string(),
//                                                   data: record.data.into_iter().collect(),
//                                                   lease: Some(
//                                                       Lease {
//                                                           stream_name: lease.stream_name.to_owned(),
//                                                           shard_id: lease.shard_id.to_owned(),
//                                                           consumer_arn: lease.consumer_arn.to_owned()
//                                                       }
//                                                     ),
//                                               }
//                                           })
//                                       );

//                                       match record_sender.send(Ok(GetRecordsResponse {
//                                           records: send_records,
//                                       })).await {
//                                           Ok(_) => (),
//                                           Err(e) => {
//                                               println!("{}", e);
//                                               break
//                                           },
//                                       }
//                                   },
//                                   _ => todo!(),
//                               }
//                           }
//                           Err(e) => {
//                               println!("{:?}", e);
//                           }
//                       }
//                     }

//                   storage_backend
//                       .release_lease(
//                           &lease.consumer_arn,
//                           &lease.stream_name,
//                           &lease.shard_id,
//                           app_name.as_str(),
//                       )
//                       .await
//                       .expect(format!("Unable to release lease: {}. This may result in shards being starved.", lease).as_str());
//             }));
//         }

//         let should_sleep = task_handles.len() == 0;

//         tokio::select! {
//           _ = &mut stream_dropped_receiver => {
//               println!("connection dropped");
//             connection_manager
//                 .remove_connection_for_streams(&streams, remote_addr)
//                 .expect(
//                     "Unable to remove connection.
//             This will result in an incorrect shard distribution across record
//             processors, which may result in shards being starved",
//                 );
//             break;
//           },
//           _ = tokio::time::sleep(Duration::from_secs(5)), if should_sleep => {
//               println!("sleeping");
//           },
//           _ = futures::future::join_all(task_handles), if !should_sleep => ()
//         }
//     }
// });
