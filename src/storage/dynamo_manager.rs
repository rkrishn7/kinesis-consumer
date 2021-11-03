use super::manager::Manager;
use rusoto_core::{Region, RusotoError};
use rusoto_dynamodb::{
    AttributeDefinition, CreateTableInput, DescribeTableError, DescribeTableInput,
    DescribeTableOutput, DynamoDb, DynamoDbClient, KeySchemaElement, TableDescription,
};

const DYNAMO_MANAGER_TABLE_NAME: &'static str = "consumer_manager";

struct DynamoManager {
    client: DynamoDbClient,
}

impl DynamoManager {
    pub fn new() -> Self {
        // TODO: Use default region
        Self {
            client: DynamoDbClient::new(Region::Custom {
                name: "us-west-2".to_owned(),
                endpoint: "".to_owned(),
            }),
        }
    }

    async fn get_manager_table(&self) -> Option<TableDescription> {
        match self
            .client
            .describe_table(DescribeTableInput {
                table_name: String::from(DYNAMO_MANAGER_TABLE_NAME),
            })
            .await
        {
            Ok(output) => output.table,
            Err(e) => {
                panic!("AWS Error: {:?}", e);
            }
        }
    }

    fn get_manager_table_input(&self) -> CreateTableInput {
        CreateTableInput {
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "consumer_name".to_string(),
                attribute_type: "S".to_string(),
            }],
            table_name: String::from(DYNAMO_MANAGER_TABLE_NAME),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "consumer_arn".to_string(),
                    key_type: "HASH".to_string(),
                },
                KeySchemaElement {
                    attribute_name: "stream_name".to_string(),
                    key_type: "Range".to_string(),
                },
                KeySchemaElement {
                    attribute_name: "shard_id".to_string(),
                    key_type: "Range".to_string(),
                },
            ],
            ..Default::default()
        }
    }

    async fn create_manager_table_if_not_exists(&self) {
        match self.get_manager_table().await {
            Some(table_description) => {
                let table_name = table_description
                    .table_name
                    .unwrap_or(String::from(DYNAMO_MANAGER_TABLE_NAME));

                println!("Skipping table creation. {} already exists.", table_name);
            }
            None => {
                let create_table_input = self.get_manager_table_input();

                match self.client.create_table(create_table_input).await {
                    Ok(_) => {
                        println!("Manager table successfully created!");
                    }
                    Err(e) => {
                        panic!("AWS Error: {:?}", e);
                    }
                }
            }
        }
    }
}

impl Manager for DynamoManager {
    fn checkpoint_consumer(&self, sequence_number: &String, consumer: &crate::proto::KdsConsumer) {
        unimplemented!()
    }
}
