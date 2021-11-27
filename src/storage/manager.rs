use crate::consumer::lease::ConsumerLease;
use crate::proto::KdsConsumer;
use async_trait::async_trait;

#[async_trait]
pub trait Manager {
    async fn checkpoint_consumer(&mut self, sequence_number: &String, consumer: &KdsConsumer);
    async fn get_available_leases(&self) -> Vec<&ConsumerLease>;
    async fn create_lease_if_not_exists(&mut self, lease: ConsumerLease);
    async fn claim_lease(&mut self, lease: ConsumerLease);
    async fn release_lease(&mut self, lease: ConsumerLease);
}

pub trait SyncManager {
    fn checkpoint_consumer(&mut self, sequence_number: &String, consumer: &KdsConsumer);
    fn get_available_leases<'a>(&'a self) -> Vec<&'a ConsumerLease>;
    fn create_lease_if_not_exists(&mut self, lease: ConsumerLease);
    fn claim_lease(&mut self, lease: ConsumerLease);
    fn release_lease(&mut self, lease: ConsumerLease);
}
