pub mod postgres;

use crate::consumer::lease::ConsumerLease;
use async_trait::async_trait;

type KinesisStorageBackendResult<T> = Result<T, Box<dyn std::error::Error>>;

#[async_trait]
pub trait AsyncKinesisStorageBackend {
    async fn checkpoint_consumer(&mut self, sequence_number: &String);
    async fn claim_available_leases(
        &self,
        limit: i64,
        stream_name: &str,
        app_name: &str,
    ) -> anyhow::Result<Vec<ConsumerLease>>;
    async fn create_lease_if_not_exists(
        &self,
        consumer_arn: &str,
        stream_name: &str,
        shard_id: &str,
        app_name: &str,
    ) -> anyhow::Result<()>;
    async fn release_lease(&self, lease: &ConsumerLease) -> anyhow::Result<()>;
    async fn get_lease_count(
        &self,
        stream_name: &str,
        app_name: &str,
    ) -> anyhow::Result<i64>;
}
