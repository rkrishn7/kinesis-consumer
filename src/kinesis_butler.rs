use std::time::Duration;

use crate::aws::kinesis;
use crate::consumer_lease::ConsumerLease;
use crate::proto::DataRecords;
use crate::proto::Lease;
use crate::record_processor::RecordProcessorRegister;
use crate::storage::KinesisStorageBackend;
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::Future;
use futures::StreamExt;
use rusoto_kinesis::KinesisClient;
use rusoto_kinesis::SubscribeToShardEventStreamItem;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct KinesisButler<T: Clone, U: Clone> {
    pub storage_backend: T,
    pub kinesis_client: KinesisClient,
    pub record_processor_register: U,
}

impl<T: Clone, U: Clone> KinesisButler<T, U> {
    pub fn new(
        storage_backend: T,
        client: KinesisClient,
        connection_manager: U,
    ) -> Self {
        Self {
            storage_backend,
            kinesis_client: client,
            record_processor_register: connection_manager,
        }
    }

    #[inline(always)]
    fn get_consumer_name(app_name: &str) -> String {
        let consumer_prefix = env!("CARGO_PKG_NAME");
        format!("{}-{}", consumer_prefix, app_name)
    }
}

impl<
        T: KinesisStorageBackend + Clone + Send + Sync + 'static,
        U: RecordProcessorRegister + Clone + Send + Sync + 'static,
    > KinesisButler<T, U>
{
    /// For a given set of streams, namespaced by `app_name`, create/update a lease entry
    /// in our database. Since this involves multiple requests to get stream metadata, create
    /// a task for each stream and run them concurrently.
    async fn refresh_consumer_leases(
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
        let connection_count = self
            .record_processor_register
            .get_count(app_name.into(), stream_name.into())
            .await as i64;

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

    pub fn listen_shutdown(&self) {
        let storage_backend = self.storage_backend.clone();

        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();

            let mut status = 0;

            if let Err(_) = storage_backend.release_claimed_leases().await {
                eprintln!("Unable to release claimed leases.");
                status = 1;
            }

            std::process::exit(status);
        });
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

        let claimed_leases = self
            .try_claim_leases_for_streams(app_name.as_str(), &streams)
            .await?;

        let mut tasks = self.send_multiple(
            claimed_leases.clone(),
            tx,
            Duration::from_secs(20),
        );
        while let Some(Ok(lease)) = tasks.next().await {
            self.storage_backend.release_lease(
                &lease
            )
            .await
            .expect(format!("Unable to release lease: {:?}. This may result in shards being starved.", lease).as_str());
        }

        Ok(())
    }
}

impl<T: Clone, U: Clone> KinesisButler<T, U> {
    fn send_records<G>(
        &self,
        lease: ConsumerLease,
        tx: Sender<G>,
        duration: Duration,
    ) -> impl Future<Output = ConsumerLease>
    where
        G: From<DataRecords>,
    {
        let consume_records_fut = self.consume_records(lease.clone());

        let tx1 = tx.clone();

        let fut = async move {
            let records_stream = consume_records_fut.await;
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
                _ = tx1.closed() => lease,
                _ = tokio::time::timeout(duration, fut) => lease,
            }
        }
    }

    fn consume_records(
        &self,
        lease: ConsumerLease,
    ) -> impl futures::Future<
        Output = impl futures::Stream<Item = Result<DataRecords, ()>>,
    > {
        let kinesis_client = self.kinesis_client.clone();

        async move {
            let starting_position_type = lease.get_starting_position_type();

            let mut event_stream = kinesis::subscribe_to_shard(
                &kinesis_client,
                lease.shard_id().to_owned(),
                lease.consumer_arn().to_owned(),
                starting_position_type.to_string(),
                None,
                lease.last_processed_sn().clone(),
            )
            .await
            .unwrap();

            let lease: Lease = lease.into();

            async_stream::stream! {
                while let Some(item) = event_stream.next().await {
                    match item {
                        Ok(
                            SubscribeToShardEventStreamItem::SubscribeToShardEvent(
                                event,
                            ),
                        ) => {
                            if event.records.len() == 0 {
                                continue;
                            }

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
}

impl<T: Clone + 'static, U: Clone + 'static> KinesisButler<T, U> {
    fn send_multiple<G>(
        &self,
        leases: Vec<ConsumerLease>,
        tx: Sender<G>,
        duration: Duration,
    ) -> FuturesUnordered<JoinHandle<ConsumerLease>>
    where
        G: From<DataRecords> + Send + 'static,
    {
        leases
            .into_iter()
            .map(|lease| {
                tokio::spawn(self.send_records(lease, tx.clone(), duration))
            })
            .collect::<FuturesUnordered<JoinHandle<ConsumerLease>>>()
    }
}
