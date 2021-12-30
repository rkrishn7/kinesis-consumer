use std::time::Duration;

use crate::aws::kinesis;
use crate::connection_manager::ConnectionManager;
use crate::consumer::lease::ConsumerLease;
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
use tokio::time::error::Elapsed;

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

    async fn try_claim_leases(
        &self,
        app_name: &str,
        stream_name: &str,
    ) -> Result<Vec<ConsumerLease>, anyhow::Error> {
        let connection_count =
            self.connection_manager
                .get_connection_count(app_name, stream_name) as i64;

        let lease_count = self
            .storage_backend
            .get_lease_count(stream_name, app_name)
            .await?;

        let limit = (lease_count + ((connection_count) - 1)) / connection_count;

        Ok(self
            .storage_backend
            .claim_available_leases(limit, stream_name, app_name)
            .await?)
    }

    async fn try_claim_leases_for_streams(
        &self,
        app_name: &str,
        streams: &Vec<String>,
    ) -> Result<Vec<ConsumerLease>, anyhow::Error> {
        let mut leases = Vec::new();
        for stream_name in streams {
            leases.extend(
                self.try_claim_leases(app_name, stream_name.as_str())
                    .await?
                    .into_iter(),
            );
        }

        Ok(leases)
    }

    pub async fn serve<G>(
        &mut self,
        app_name: String,
        streams: Vec<String>,
        tx: Sender<G>,
    ) -> Result<(), anyhow::Error>
    where
        G: From<DataRecords> + Send + 'static,
    {
        self.refresh_consumer_leases(streams.clone(), app_name.clone())
            .await?;

        self.connection_manager
            .increment_connections(app_name.as_str(), &streams);
        loop {
            if tx.is_closed() {
                self.connection_manager
                    .decrement_connections(app_name.as_str(), &streams);
                break;
            }

            let claimed_leases = self
                .try_claim_leases_for_streams(app_name.as_str(), &streams)
                .await?;

            println!("leases {:?}", claimed_leases);

            if claimed_leases.len() == 0 {
                tokio::time::sleep(Duration::from_secs(5)).await;
            } else {
                try_join_all(
                    self.send_multiple(claimed_leases.clone(), tx.clone()),
                )
                .await;
                for lease in claimed_leases.iter() {
                    self.storage_backend.release_lease(
                        &lease
                    )
                    .await
                    .expect(format!("Unable to release lease: {}. This may result in shards being starved.", lease).as_str());
                }

                println!("howdy");
            }
        }

        Ok(())
    }
}

impl<T: Clone, U: Clone> KinesisConsumerService<T, U> {
    fn send_records<G>(
        &self,
        lease: ConsumerLease,
        tx: Sender<G>,
    ) -> impl Future<Output = ()>
    where
        G: From<DataRecords>,
    {
        let records_stream = self.consume_records(lease);

        let tx1 = tx.clone();

        let fut = async move {
            tokio::pin!(records_stream);

            while let Some(result) = records_stream.next().await {
                if let Ok(records) = result {
                    if let Err(_) = tx.send(records.into()).await {
                        break;
                    }
                } else {
                    break;
                }
            }
        };

        async move {
            tokio::select! {
                _ = tx1.closed() => (),
                _ = fut => (),
            }
        }
    }

    fn consume_records(
        &self,
        lease: ConsumerLease,
    ) -> impl futures::Stream<Item = Result<DataRecords, ()>> {
        let kinesis_client = self.kinesis_client.clone();

        async_stream::stream! {
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

            let lease: Lease = lease.into();

            while let Some(item) = event_stream.next().await {
                match item {
                    Ok(
                        SubscribeToShardEventStreamItem::SubscribeToShardEvent(
                            event,
                        ),
                    ) => {
                        let mut records: DataRecords = event.records.into();
                        records.attach_lease(&lease);

                        yield Ok(records);
                    }
                    Ok(_event_stream_error) => {
                        yield Err(());
                    }
                    Err(_rusoto_error) => {
                        yield Err(());
                    }
                }
            }
        }
    }
}

impl<T: Clone + 'static, U: Clone + 'static> KinesisConsumerService<T, U> {
    fn send_multiple<G>(
        &self,
        leases: Vec<ConsumerLease>,
        tx: Sender<G>,
    ) -> FuturesUnordered<JoinHandle<Result<(), Elapsed>>>
    where
        G: From<DataRecords> + Send + 'static,
    {
        leases
            .into_iter()
            .map(|lease| {
                tokio::spawn(tokio::time::timeout(
                    Duration::from_secs(20),
                    self.send_records(lease, tx.clone()),
                ))
            })
            .collect::<FuturesUnordered<JoinHandle<Result<(), Elapsed>>>>()
    }
}
