#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ConsumerLease {
    pub consumer_arn: String,
    pub shard_id: String,
    pub leased: bool,
    pub stream_name: String,
    pub last_processed_sn: Option<String>,
}

impl ConsumerLease {
    pub fn new(
        consumer_arn: String,
        stream_name: String,
        shard_id: String,
    ) -> Self {
        Self {
            consumer_arn,
            stream_name,
            leased: false,
            last_processed_sn: None,
            shard_id,
        }
    }
}
