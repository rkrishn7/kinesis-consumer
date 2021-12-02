use super::manager::SyncManager;
use crate::consumer::lease::ConsumerLease;
use crate::proto::KdsConsumer;

pub struct MemoryManager {
    pub leases: Vec<ConsumerLease>,
}

impl MemoryManager {
    pub fn new() -> Self {
        Self { leases: Vec::new() }
    }
}

impl SyncManager for MemoryManager {
    fn checkpoint_consumer(&mut self, sequence_number: &String, consumer: &KdsConsumer) {
        match self
            .leases
            .iter_mut()
            .find(|lease| lease.consumer_arn == consumer.arn)
        {
            Some(lease) => {
                lease.last_processed_sn = Some(sequence_number.clone());
            }
            None => {}
        }
    }

    fn get_available_leases(&self) -> Vec<&ConsumerLease> {
        self.leases
            .iter()
            .filter(|&lease| lease.state == "AVAILABLE")
            .collect()
    }

    fn available_leases_mut(&mut self) -> Vec<&mut ConsumerLease> {
        self.leases
            .iter_mut()
            .filter(|lease| lease.state == "AVAILABLE")
            .collect()
    }

    fn create_lease_if_not_exists(&mut self, lease: ConsumerLease) {
        match self
            .leases
            .iter()
            .find(|&l| l.consumer_arn == lease.consumer_arn && l.shard_id == lease.shard_id)
        {
            None => self.leases.push(lease),
            Some(_) => {}
        }
    }

    fn claim_lease(&mut self, lease: ConsumerLease) {
        match self
            .leases
            .iter_mut()
            .find(|l| l.consumer_arn == lease.consumer_arn)
        {
            None => panic!("Lease does not exist"),
            Some(lease) => lease.state = "NOT_AVAILABLE".to_string(),
        }
    }

    fn release_lease(&mut self, lease: ConsumerLease) {
        match self
            .leases
            .iter_mut()
            .find(|l| l.consumer_arn == lease.consumer_arn)
        {
            None => panic!("Lease does not exist"),
            Some(lease) => lease.state = "AVAILABLE".to_string(),
        }
    }
}
