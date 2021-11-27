use crate::consumer::lease::ConsumerLease;
use std::collections::HashMap;

use super::manager::Manager;
use rusoto_core::{Region, RusotoError};
use rusoto_dynamodb::{
    AttributeDefinition, AttributeValue, CreateTableInput, DescribeTableError, DescribeTableInput,
    DynamoDb, DynamoDbClient, GlobalSecondaryIndex, KeySchemaElement, Projection,
    ProvisionedThroughput, PutItemInput, TableDescription,
};

use async_trait::async_trait;
use rusoto_kinesis::Consumer;

const DYNAMO_MANAGER_TABLE_NAME: &'static str = "consumer_leases";

pub struct DynamoManager {
    client: DynamoDbClient,
}

impl DynamoManager {
    pub fn new() -> Self {
        // TODO: Use default region
        Self {
            client: DynamoDbClient::new(Region::Custom {
                name: "us-west-2".to_owned(),
                endpoint: "http://localhost:4566".to_owned(),
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
            Err(rusoto_err) => match rusoto_err {
                RusotoError::Service(describe_table_err) => match describe_table_err {
                    DescribeTableError::ResourceNotFound(_) => None,
                    DescribeTableError::InternalServerError(msg) => {
                        panic!("AWS Error: {:?}", msg);
                    }
                },
                _ => {
                    panic!("AWS Error: {:?}", rusoto_err);
                }
            },
        }
    }

    fn get_manager_table_input(&self) -> CreateTableInput {
        CreateTableInput {
            attribute_definitions: vec![
                AttributeDefinition {
                    attribute_name: "consumer_arn".to_string(),
                    attribute_type: "S".to_string(),
                },
                AttributeDefinition {
                    attribute_name: "shard_id".to_string(),
                    attribute_type: "S".to_string(),
                },
                AttributeDefinition {
                    attribute_name: "state".to_string(),
                    attribute_type: "S".to_string(),
                },
            ],
            table_name: String::from(DYNAMO_MANAGER_TABLE_NAME),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "consumer_arn".to_string(),
                    key_type: "HASH".to_string(),
                },
                KeySchemaElement {
                    attribute_name: "shard_id".to_string(),
                    key_type: "RANGE".to_string(),
                },
            ],
            global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
                index_name: "state".to_string(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "state".to_string(),
                    key_type: "HASH".to_string(),
                }],
                projection: Projection {
                    projection_type: Some("KEYS_ONLY".to_string()),
                    non_key_attributes: None,
                },
                provisioned_throughput: Some(ProvisionedThroughput {
                    read_capacity_units: 15,
                    write_capacity_units: 15,
                }),
            }]),
            provisioned_throughput: Some(ProvisionedThroughput {
                read_capacity_units: 15,
                write_capacity_units: 15,
            }),
            ..Default::default()
        }
    }

    pub async fn create_manager_table_if_not_exists(&self) {
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

#[async_trait]
impl Manager for DynamoManager {
    async fn checkpoint_consumer(
        &mut self,
        sequence_number: &String,
        consumer: &crate::proto::KdsConsumer,
    ) {
        unimplemented!()
    }

    async fn get_available_leases(&self) -> Vec<&ConsumerLease> {
        unimplemented!()
    }

    async fn create_lease_if_not_exists(&mut self, consumer_lease: ConsumerLease) {
        // let record_input = PutItemInput {
        //     table_name: DYNAMO_MANAGER_TABLE_NAME.to_string(),
        //     item: HashMap::from([
        //         (
        //             "consumer_arn".to_string(),
        //             AttributeValue {
        //                 s: Some(consumer_lease.consumer_arn),
        //                 ..Default::default()
        //             },
        //         ),
        //         (
        //             "shard_id".to_string(),
        //             AttributeValue {
        //                 s: Some(consumer_lease.shard_id),
        //                 ..Default::default()
        //             },
        //         ),
        //         (
        //             "state".to_string(),
        //             AttributeValue {
        //                 s: Some("IDLE".to_string()),
        //                 ..Default::default()
        //             },
        //         ),
        //     ]),
        //     ..Default::default()
        // };

        // match self.client.put_item(record_input).await {
        //     Ok(output) => {
        //         println!("consumer lease created succesfully!")
        //     }
        //     Err(e) => {
        //         panic!("AWS Error {:?}", e);
        //     }
        // }

        todo!()
    }

    async fn claim_lease(&mut self, consumer_lease: ConsumerLease) {
        unimplemented!()
    }

    async fn release_lease(&mut self, consumer_lease: ConsumerLease) {
        unimplemented!()
    }
}
