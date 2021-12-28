tonic::include_proto!("kinesisbutler");

pub struct DataRecords(pub Vec<DataRecord>);

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
