use std::str::FromStr;

use crate::consumer_lease::ConsumerLease;

tonic::include_proto!("kinesisbutler");

pub struct DataRecords(pub Vec<DataRecord>);

impl DataRecords {
    pub fn attach_lease(&mut self, lease: &Lease) {
        let records = &mut self.0;
        records.into_iter().for_each(|record| {
            record.lease = Some(lease.clone());
        });
    }
}

impl IntoIterator for DataRecords {
    type Item = DataRecord;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<Vec<DataRecord>> for DataRecords {
    fn from(records: Vec<DataRecord>) -> Self {
        Self(records)
    }
}

impl From<DataRecords> for Result<GetRecordsResponse, tonic::Status> {
    fn from(w: DataRecords) -> Self {
        Ok(GetRecordsResponse { records: w.0 })
    }
}

impl From<ConsumerLease> for Lease {
    fn from(lease: ConsumerLease) -> Self {
        Self {
            stream_name: lease.stream_name().to_owned(),
            shard_id: lease.shard_id().to_owned(),
            consumer_arn: lease.consumer_arn().to_owned(),
            app_name: lease.app_name().to_owned(),
            instance_id: lease.instance_id().clone().to_string(),
        }
    }
}

impl From<rusoto_kinesis::Record> for DataRecord {
    fn from(record: rusoto_kinesis::Record) -> Self {
        Self {
            sequence_number: record.sequence_number,
            timestamp: record
                .approximate_arrival_timestamp
                .map(|t| t.to_string())
                .unwrap_or(String::from("")),
            partition_key: record.partition_key,
            encryption_type: record.encryption_type.unwrap_or(String::from("")),
            data: record.data.into_iter().collect(),
            lease: None,
        }
    }
}

impl From<Vec<rusoto_kinesis::Record>> for DataRecords {
    fn from(records: Vec<rusoto_kinesis::Record>) -> Self {
        Self(records.into_iter().map(|record| record.into()).collect())
    }
}
