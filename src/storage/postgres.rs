use super::AsyncKinesisStorageBackend;
use crate::consumer::lease::ConsumerLease;
use async_trait::async_trait;

use std::{env, sync::Arc};

const CONSUMER_LEASES_TABLE_NAME: &'static str =
    "kinesis_butler_consumer_leases";

#[derive(Clone)]
pub struct PostgresKinesisStorageBackend {
    client: Arc<tokio_postgres::Client>,
}

impl PostgresKinesisStorageBackend {
    pub async fn init() -> Self {
        let postgres_url = env::var("POSTGRES_DATABASE_URL").expect("Expected environment variable `POSTGRES_DATABASE_URL` when using value postgres as a storage backend");
        let (client, connection) =
            tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls)
                .await
                .expect("Unable to initialize postgres client");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        client
            .batch_execute(
                format!(
                    "CREATE TABLE IF NOT EXISTS {} ( 
              id                  SERIAL NOT NULL PRIMARY KEY,
              consumer_arn        VARCHAR(255) NOT NULL,
              shard_id            VARCHAR(255) NOT NULL,
              stream_name         VARCHAR(255) NOT NULL,
              leased              BOOLEAN DEFAULT false,
              last_processed_sn   VARCHAR(255) DEFAULT NULL,
              UNIQUE (consumer_arn, shard_id, stream_name)
            )",
                    CONSUMER_LEASES_TABLE_NAME
                )
                .as_str(),
            )
            .await
            .expect("Unable to create consumer leases table");

        Self {
            client: Arc::new(client),
        }
    }
}

#[async_trait]
impl AsyncKinesisStorageBackend for PostgresKinesisStorageBackend {
    async fn checkpoint_consumer(&mut self, sequence_number: &String) {
        unimplemented!()
    }

    async fn claim_available_leases_for_streams(
        &self,
        limit: i64,
        streams: &Vec<&str>,
    ) -> Result<Vec<ConsumerLease>, Box<dyn std::error::Error>> {
        let rows = self.client.query(format!(
                "UPDATE {0} SET leased = true WHERE id in (SELECT id FROM {0} WHERE stream_name = ANY($1) AND leased = false LIMIT $2) RETURNING *", CONSUMER_LEASES_TABLE_NAME).as_str(),
                &[streams, &limit],
            )
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| ConsumerLease {
                consumer_arn: row.get("consumer_arn"),
                shard_id: row.get("shard_id"),
                leased: row.get("leased"),
                stream_name: row.get("stream_name"),
                last_processed_sn: row.get("last_processed_sn"),
            })
            .collect())
    }

    async fn create_lease_if_not_exists(
        &self,
        consumer_arn: &str,
        stream_name: &str,
        shard_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self
            .client
            .execute(
                format!("INSERT INTO {} (consumer_arn, shard_id, stream_name) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING", CONSUMER_LEASES_TABLE_NAME).as_str(),
                &[
                    &consumer_arn,
                    &shard_id,
                    &stream_name,
                ],
            )
            .await?;

        Ok(())
    }

    async fn release_lease(
        &self,
        consumer_arn: &str,
        stream_name: &str,
        shard_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let updated = self
            .client
            .execute(
                format!(
                    "UPDATE {} SET leased = false WHERE 
                consumer_arn = $1 AND shard_id = $2 AND stream_name = $3
                ",
                    CONSUMER_LEASES_TABLE_NAME
                )
                .as_str(),
                &[&consumer_arn, &shard_id, &stream_name],
            )
            .await?;

        println!("Updated {}", updated);

        Ok(())
    }

    async fn get_lease_count_for_streams(
        &self,
        streams: &Vec<&str>,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let row = self.client.query_one(format!(
        "SELECT COUNT(*) FROM {0} WHERE id in (SELECT id FROM {0} WHERE stream_name = ANY($1))", CONSUMER_LEASES_TABLE_NAME).as_str(),
        &[streams],
        )
        .await?;

        Ok(row.get(0))
    }
}
