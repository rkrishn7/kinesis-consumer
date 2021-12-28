#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ConsumerLease {
    pub consumer_arn: String,
    pub app_name: String,
    pub shard_id: String,
    pub process_id: uuid::Uuid,
    pub stream_name: String,
    pub last_processed_sn: Option<String>,
}

impl ConsumerLease {
    pub fn get_starting_position_type(&self) -> &'static str {
        if self.last_processed_sn.is_none() {
            return "TRIM_HORIZON";
        }

        return "AFTER_SEQUENCE_NUMBER";
    }
}

impl std::fmt::Display for ConsumerLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(stream_name: {}, shard_id: {}, consumer_arn: {}, app_name: {})",
            &self.stream_name,
            &self.shard_id,
            &self.consumer_arn,
            &self.app_name
        )
    }
}
