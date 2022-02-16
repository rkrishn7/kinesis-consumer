use anyhow::Context;
use rusoto_core::event_stream::EventStream;
use rusoto_core::{Region, RusotoError};
use rusoto_kinesis::{
    Consumer, ConsumerDescription, DescribeStreamInput, ListShardsInput,
    RegisterStreamConsumerError, RegisterStreamConsumerInput, Shard,
    StartingPosition, StreamDescription, SubscribeToShardError,
    SubscribeToShardEventStreamItem,
};
use rusoto_kinesis::{Kinesis, KinesisClient};

pub const MAX_REGISTER_STREAM_CONSUMER_TRANSACTIONS_PER_SECOND: usize = 5;

pub fn create_client() -> KinesisClient {
    KinesisClient::new(Region::default())
}

pub async fn describe_stream(
    client: &KinesisClient,
    stream_name: String,
    exclusive_start_shard_id: Option<String>,
    shard_limit: Option<i64>,
) -> Result<StreamDescription, anyhow::Error> {
    let input = DescribeStreamInput {
        exclusive_start_shard_id,
        limit: shard_limit,
        stream_name,
    };

    Ok(client.describe_stream(input).await?.stream_description)
}

pub async fn list_shards(
    client: &KinesisClient,
    stream_name: String,
) -> Result<Vec<Shard>, anyhow::Error> {
    let mut next_token = None;
    let mut all_shards = Vec::new();

    loop {
        let input = ListShardsInput {
            exclusive_start_shard_id: None,
            max_results: None,
            next_token,
            shard_filter: None,
            stream_creation_timestamp: None,
            stream_name: Some(stream_name.clone()),
        };
        let output = client.list_shards(input).await.with_context(|| {
            format!("Error listing shards for stream {}", stream_name)
        })?;

        if let Some(shards) = output.shards {
            all_shards.extend(shards);
        }

        if output.next_token.is_some() {
            next_token = output.next_token;
        } else {
            break Ok(all_shards);
        }
    }
}

pub async fn get_shard_ids(
    client: &KinesisClient,
    stream_name: String,
) -> Result<impl Iterator<Item = String>, anyhow::Error> {
    let shards = list_shards(client, stream_name).await?;
    Ok(shards.into_iter().map(|s| s.shard_id))
}

pub async fn subscribe_to_shard(
    client: &KinesisClient,
    shard_id: String,
    consumer_arn: String,
    starting_position_type: String,
    timestamp: Option<f64>,
    sequence_number: Option<String>,
) -> Result<
    EventStream<SubscribeToShardEventStreamItem>,
    RusotoError<SubscribeToShardError>,
> {
    let input = rusoto_kinesis::SubscribeToShardInput {
        consumer_arn,
        shard_id,
        starting_position: StartingPosition {
            type_: starting_position_type,
            timestamp,
            sequence_number,
        },
    };

    Ok(client.subscribe_to_shard(input).await?.event_stream)
}

pub async fn register_stream_consumer(
    client: &KinesisClient,
    consumer_name: String,
    stream_arn: String,
) -> Result<Consumer, anyhow::Error> {
    let error_msg = format!(
        "Error registering stream consumer {} for stream {}",
        consumer_name, stream_arn
    );

    let input = RegisterStreamConsumerInput {
        consumer_name,
        stream_arn,
    };

    Ok(client
        .register_stream_consumer(input)
        .await
        .context(error_msg)?
        .consumer)
}

pub async fn get_or_register_stream_consumer(
    client: &KinesisClient,
    consumer_name: String,
    stream_arn: String,
) -> Result<Consumer, anyhow::Error> {
    match register_stream_consumer(
        client,
        consumer_name.to_owned(),
        stream_arn.to_owned(),
    )
    .await
    {
        Ok(consumer) => Ok(consumer),
        Err(e) => {
            // If the consumer has already been registered, attempt to
            // synthesize a `Consumer` from its description
            if let Some(RusotoError::Service(
                RegisterStreamConsumerError::ResourceInUse(_),
            )) = e.downcast_ref::<RusotoError<RegisterStreamConsumerError>>()
            {
                let consumer_description =
                    describe_stream_consumer(client, consumer_name, stream_arn)
                        .await?;

                Ok(Consumer {
                    consumer_arn: consumer_description.consumer_arn,
                    consumer_creation_timestamp: consumer_description
                        .consumer_creation_timestamp,
                    consumer_name: consumer_description.consumer_name,
                    consumer_status: consumer_description.consumer_status,
                })
            } else {
                Err(e)
            }
        }
    }
}

pub async fn describe_stream_consumer(
    client: &KinesisClient,
    consumer_name: String,
    stream_arn: String,
) -> Result<ConsumerDescription, anyhow::Error> {
    let error_msg = format!(
        "Error describing stream consumer {} for stream {}",
        consumer_name, stream_arn
    );
    let input = rusoto_kinesis::DescribeStreamConsumerInput {
        consumer_name: Some(consumer_name),
        stream_arn: Some(stream_arn),
        ..rusoto_kinesis::DescribeStreamConsumerInput::default()
    };

    Ok(client
        .describe_stream_consumer(input)
        .await
        .context(error_msg)?
        .consumer_description)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusoto_mock::{MockCredentialsProvider, MockRequestDispatcher};

    #[tokio::test]
    async fn test_describe_stream() {
        let stream_description = rusoto_kinesis::StreamDescription {
            has_more_shards: false,
            retention_period_hours: 24,
            stream_arn: String::from("stream-arn"),
            stream_creation_timestamp: 111222333.0,
            stream_name: String::from("stream-name"),
            stream_status: String::from("ACTIVE"),
            ..Default::default()
        };

        let client = KinesisClient::new_with(
            MockRequestDispatcher::default().with_json_body(
                rusoto_kinesis::DescribeStreamOutput {
                    stream_description: stream_description.clone(),
                },
            ),
            MockCredentialsProvider,
            Region::default(),
        );

        let output =
            describe_stream(&client, "fake_stream".into(), None, None).await;

        assert_eq!(output.unwrap(), stream_description);
    }

    #[tokio::test]
    async fn test_describe_stream_service_error() {
        let client = KinesisClient::new_with(
            MockRequestDispatcher::with_status(500),
            MockCredentialsProvider,
            Region::default(),
        );

        let output =
            describe_stream(&client, "fake_stream".into(), None, None).await;

        assert!(output.is_err());
    }

    #[tokio::test]
    async fn test_list_shards() {
        let shards = vec![rusoto_kinesis::Shard {
            hash_key_range: rusoto_kinesis::HashKeyRange {
                ending_hash_key: String::from(""),
                starting_hash_key: String::from(""),
            },
            sequence_number_range: rusoto_kinesis::SequenceNumberRange {
                ending_sequence_number: None,
                starting_sequence_number: String::from("1"),
            },
            shard_id: String::from("shard-0001"),
            ..Default::default()
        }];

        let client = KinesisClient::new_with(
            MockRequestDispatcher::default().with_json_body(
                rusoto_kinesis::ListShardsOutput {
                    next_token: None,
                    shards: Some(shards.clone()),
                },
            ),
            MockCredentialsProvider,
            Region::default(),
        );

        let output = list_shards(&client, "fake-stream".into()).await;

        assert_eq!(output.unwrap(), shards);
    }

    #[tokio::test]
    async fn test_get_shard_ids() {
        let shards = vec![rusoto_kinesis::Shard {
            hash_key_range: rusoto_kinesis::HashKeyRange {
                ending_hash_key: String::from(""),
                starting_hash_key: String::from(""),
            },
            sequence_number_range: rusoto_kinesis::SequenceNumberRange {
                ending_sequence_number: None,
                starting_sequence_number: String::from("1"),
            },
            shard_id: String::from("shard-0001"),
            ..Default::default()
        }];

        let client = KinesisClient::new_with(
            MockRequestDispatcher::default().with_json_body(
                rusoto_kinesis::ListShardsOutput {
                    next_token: None,
                    shards: Some(shards.clone()),
                },
            ),
            MockCredentialsProvider,
            Region::default(),
        );

        let output = get_shard_ids(&client, "fake-stream".into()).await;

        assert_eq!(
            output.unwrap().collect::<Vec<String>>(),
            vec![String::from("shard-0001")]
        );
    }
}
