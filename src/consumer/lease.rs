#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ConsumerLease {
    pub consumer_arn: String,
    pub shard_id: String,
    pub state: String,
    pub stream_name: String,
    pub last_processed_sn: Option<String>,
}

impl ConsumerLease {
    pub fn new(consumer_arn: String, stream_name: String, shard_id: String) -> Self {
        Self {
            consumer_arn,
            stream_name,
            state: "AVAILABLE".to_string(),
            last_processed_sn: None,
            shard_id: shard_id,
        }
    }

    pub fn claim(&mut self) {
        self.state = "LEASED".to_string();
    }
}
