#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ConsumerLease {
    pub consumer_arn: String,
    pub shard_id: String,
    pub consumer_id: uuid::Uuid,
    pub stream_name: String,
    pub last_processed_sn: Option<String>,
}

impl std::fmt::Display for ConsumerLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(stream_name: {}, shard_id: {}, consumer_arn: {})",
            &self.stream_name, &self.shard_id, &self.consumer_arn
        )
    }
}
