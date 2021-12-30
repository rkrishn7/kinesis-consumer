pub mod postgres;

use crate::consumer::lease::ConsumerLease;
use async_trait::async_trait;

use std::error::Error as StdError;

#[async_trait]
pub trait KinesisStorageBackend {
    type Error: StdError + Send + Sync;

    async fn checkpoint_consumer(&mut self, sequence_number: &String);
    async fn claim_available_leases(
        &self,
        limit: i64,
        stream_name: &str,
        app_name: &str,
    ) -> Result<Vec<ConsumerLease>, Self::Error>;
    async fn create_lease_if_not_exists(
        &self,
        consumer_arn: &str,
        stream_name: &str,
        shard_id: &str,
        app_name: &str,
    ) -> Result<(), Self::Error>;
    async fn release_lease(
        &self,
        lease: &ConsumerLease,
    ) -> Result<(), Self::Error>;
    async fn release_claimed_leases(&self) -> Result<(), Self::Error>;
    async fn get_lease_count(
        &self,
        stream_name: &str,
        app_name: &str,
    ) -> Result<i64, Self::Error>;
}
