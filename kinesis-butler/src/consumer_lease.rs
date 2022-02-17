use std::str::FromStr;

use crate::proto::Lease;

#[derive(PartialEq, Eq, Clone, Debug, sqlx::FromRow)]
pub struct ConsumerLease {
    app_name: String,
    consumer_arn: String,
    last_processed_sn: Option<String>,
    instance_id: uuid::Uuid,
    shard_id: String,
    stream_name: String,
}

impl ConsumerLease {
    pub fn get_starting_position_type(&self) -> &'static str {
        if self.last_processed_sn.is_none() {
            return "TRIM_HORIZON";
        }

        return "AFTER_SEQUENCE_NUMBER";
    }

    pub fn app_name(&self) -> &String {
        &self.app_name
    }

    pub fn consumer_arn(&self) -> &String {
        &self.consumer_arn
    }

    pub fn last_processed_sn(&self) -> &Option<String> {
        &self.last_processed_sn
    }

    pub fn instance_id(&self) -> &uuid::Uuid {
        &self.instance_id
    }

    pub fn shard_id(&self) -> &String {
        &self.shard_id
    }

    pub fn stream_name(&self) -> &String {
        &self.stream_name
    }
}

impl TryFrom<Lease> for ConsumerLease {
    type Error = uuid::Error;

    fn try_from(lease: Lease) -> Result<Self, Self::Error> {
        Ok(Self {
            stream_name: lease.stream_name,
            shard_id: lease.shard_id,
            consumer_arn: lease.consumer_arn,
            app_name: lease.app_name,
            instance_id: uuid::Uuid::from_str(lease.instance_id.as_str())?,
            last_processed_sn: None,
        })
    }
}
