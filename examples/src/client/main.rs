use kinesis_butler::proto::consumer_service_client::ConsumerServiceClient;
use kinesis_butler::proto::{
    CheckpointLeaseRequest, GetRecordsRequest, InitializeRequest,
};

use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client =
        ConsumerServiceClient::connect("http://[::1]:50051").await?;

    let stream = "test-kinesis-consumer";

    println!("Initializing Record Processor");

    client
        .initialize(tonic::Request::new(InitializeRequest {
            app_name: "rohan".into(),
            streams: vec![stream.into()],
        }))
        .await?;

    println!("Stream initialized. Getting records from stream");

    loop {
        let mut stream = client
            .get_records(tonic::Request::new(GetRecordsRequest {
                app_name: "rohan".into(),
                streams: vec![stream.into()],
            }))
            .await?
            .into_inner();

        while let Some(res) = stream.next().await {
            if let Ok(msg) = res {
                println!("Got {} records", msg.records.len());

                if let Some(last) = msg.records.last() {
                    client
                        .checkpoint_lease(tonic::Request::new(
                            CheckpointLeaseRequest {
                                lease: last.lease.clone(),
                                sequence_number: last.sequence_number.clone(),
                            },
                        ))
                        .await?;

                    println!("Checkpointed lease at {}", last.sequence_number);
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}
