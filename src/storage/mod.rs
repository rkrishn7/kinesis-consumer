pub mod postgres;

use crate::consumer::lease::ConsumerLease;
use async_trait::async_trait;

type KinesisStorageBackendResult<T> = Result<T, Box<dyn std::error::Error>>;

#[async_trait]
pub trait AsyncKinesisStorageBackend {
    async fn checkpoint_consumer(&mut self, sequence_number: &String);
    async fn claim_available_leases_for_streams(
        &self,
        limit: i64,
        streams: &Vec<&str>,
    ) -> KinesisStorageBackendResult<Vec<ConsumerLease>>;
    async fn create_lease_if_not_exists(
        &self,
        consumer_arn: &str,
        stream_name: &str,
        shard_id: &str,
    ) -> KinesisStorageBackendResult<()>;
    async fn release_lease(
        &self,
        consumer_arn: &str,
        stream_name: &str,
        shard_id: &str,
    ) -> KinesisStorageBackendResult<()>;
    async fn get_lease_count_for_streams(
        &self,
        streams: &Vec<&str>,
    ) -> KinesisStorageBackendResult<i64>;
}
