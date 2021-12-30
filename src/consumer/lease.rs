#[derive(PartialEq, Eq, Clone, Debug, sqlx::FromRow)]
pub struct ConsumerLease {
    app_name: String,
    consumer_arn: String,
    last_processed_sn: Option<String>,
    process_id: uuid::Uuid,
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

    pub fn process_id(&self) -> &uuid::Uuid {
        &self.process_id
    }

    pub fn shard_id(&self) -> &String {
        &self.shard_id
    }

    pub fn stream_name(&self) -> &String {
        &self.stream_name
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
