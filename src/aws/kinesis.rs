use rusoto_core::event_stream::EventStream;
use rusoto_core::{Region, RusotoError};
use rusoto_kinesis::{
    Consumer, ConsumerDescription, DescribeStreamConsumerError,
    DescribeStreamConsumerInput, DescribeStreamError, DescribeStreamInput,
    ListShardsError, ListShardsInput, ListStreamConsumersError,
    ListStreamConsumersInput, RegisterStreamConsumerError,
    RegisterStreamConsumerInput, Shard, StartingPosition, StreamDescription,
    SubscribeToShardError, SubscribeToShardEventStreamItem,
};
use rusoto_kinesis::{Kinesis, KinesisClient};

pub fn create_client() -> KinesisClient {
    KinesisClient::new(Region::default())
}

pub async fn describe_stream(
    client: &KinesisClient,
    stream_name: String,
    exclusive_start_shard_id: Option<String>,
    shard_limit: Option<i64>,
) -> Result<StreamDescription, RusotoError<DescribeStreamError>> {
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
) -> Result<Vec<Shard>, RusotoError<ListShardsError>> {
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
        let output = client.list_shards(input).await?;

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
) -> Result<impl Iterator<Item = String>, RusotoError<ListShardsError>> {
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
) -> Result<Consumer, RusotoError<RegisterStreamConsumerError>> {
    let input = RegisterStreamConsumerInput {
        consumer_name,
        stream_arn,
    };

    Ok(client.register_stream_consumer(input).await?.consumer)
}

pub async fn register_stream_consumer_if_not_exists(
    client: &KinesisClient,
    consumer_name: String,
    stream_arn: String,
) -> Result<(), RusotoError<RegisterStreamConsumerError>> {
    match register_stream_consumer(client, consumer_name, stream_arn).await {
        Ok(_) => Ok(()),
        Err(RusotoError::Service(
            RegisterStreamConsumerError::ResourceInUse(_),
        )) => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn describe_stream_consumer(
    client: &KinesisClient,
    consumer_name: Option<String>,
    stream_arn: Option<String>,
    consumer_arn: Option<String>,
) -> Result<ConsumerDescription, RusotoError<DescribeStreamConsumerError>> {
    let input = DescribeStreamConsumerInput {
        consumer_name,
        stream_arn,
        consumer_arn,
    };

    Ok(client
        .describe_stream_consumer(input)
        .await?
        .consumer_description)
}

pub async fn list_stream_consumers(
    client: &KinesisClient,
    stream_arn: String,
) -> Result<Vec<Consumer>, RusotoError<ListStreamConsumersError>> {
    let mut next_token = None;
    let mut all_consumers = Vec::new();

    loop {
        let input = ListStreamConsumersInput {
            max_results: None,
            next_token,
            stream_arn: stream_arn.clone(),
            stream_creation_timestamp: None,
        };

        let output = client.list_stream_consumers(input).await?;

        if let Some(consumers) = output.consumers {
            all_consumers.extend(consumers);
        }

        if output.next_token.is_some() {
            next_token = output.next_token;
        } else {
            break Ok(all_consumers);
        }
    }
}
