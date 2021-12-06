use super::AsyncKinesisStorageBackend;
use crate::consumer::lease::ConsumerLease;
use async_trait::async_trait;

use std::sync::Arc;

const CONSUMER_LEASES_TABLE_NAME: &'static str =
    "kinesis_butler_consumer_leases";

#[derive(Clone)]
pub struct PostgresKinesisStorageBackend {
    client: Option<Arc<tokio_postgres::Client>>,
    connection_url: String,
    consumer_id: &'static uuid::Uuid,
}

impl PostgresKinesisStorageBackend {
    pub fn new(
        consumer_id: &'static uuid::Uuid,
        connection_url: String,
    ) -> Self {
        Self {
            client: None,
            consumer_id,
            connection_url,
        }
    }

    pub async fn init(&mut self) -> Result<(), tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::connect(
            &self.connection_url,
            tokio_postgres::NoTls,
        )
        .await?;

        self.client = Some(Arc::new(client));

        let client = self.client.clone();
        let consumer_id = self.consumer_id.clone();

        // Release all leases claimed by this consumer, if any,
        // upon graceful shutdown.
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            match client
                .as_ref()
                .unwrap()
                .execute(
                    format!(
                        "UPDATE {} SET consumer_id = NULL WHERE consumer_id = $1",
                        CONSUMER_LEASES_TABLE_NAME
                    )
                    .as_str(),
                    &[&consumer_id],
                )
                .await {
                    Ok(_) => {
                        println!("Released claimed leases for consumer process {}", consumer_id);
                        std::process::exit(0)
                    },
                    Err(e) => {
                        eprintln!("Unable to release claimed leases for consumer process {}. Caused by error: {}", consumer_id, e);
                        std::process::exit(1);
                    },
                }
        });

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        self.client
            .as_ref()
            .unwrap()
            .batch_execute(
                format!(
                    "CREATE TABLE IF NOT EXISTS {} ( 
              id                  SERIAL NOT NULL PRIMARY KEY,
              consumer_arn        VARCHAR(255) NOT NULL,
              shard_id            VARCHAR(255) NOT NULL,
              stream_name         VARCHAR(255) NOT NULL,
              consumer_id         UUID DEFAULT NULL,
              last_processed_sn   VARCHAR(255) DEFAULT NULL,
              UNIQUE (consumer_arn, shard_id, stream_name)
            )",
                    CONSUMER_LEASES_TABLE_NAME
                )
                .as_str(),
            )
            .await?;

        Ok(())
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
        let rows = self.client.as_ref().unwrap().query(format!(
                "UPDATE {0} SET consumer_id = $1 WHERE id in (SELECT id FROM {0} WHERE stream_name = ANY($2) AND consumer_id IS NULL LIMIT $3) RETURNING *", CONSUMER_LEASES_TABLE_NAME).as_str(),
                &[&self.consumer_id, streams, &limit],
            )
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| ConsumerLease {
                consumer_arn: row.get("consumer_arn"),
                shard_id: row.get("shard_id"),
                consumer_id: row.get("consumer_id"),
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
            .as_ref()
            .unwrap()
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
        self.client
            .as_ref()
            .unwrap()
            .execute(
                format!(
                    "UPDATE {} SET consumer_id = NULL WHERE 
                consumer_arn = $1 AND shard_id = $2 AND stream_name = $3
                ",
                    CONSUMER_LEASES_TABLE_NAME
                )
                .as_str(),
                &[&consumer_arn, &shard_id, &stream_name],
            )
            .await?;

        Ok(())
    }

    async fn get_lease_count_for_streams(
        &self,
        streams: &Vec<&str>,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let row = self.client.as_ref().unwrap().query_one(format!(
        "SELECT COUNT(*) FROM {0} WHERE id in (SELECT id FROM {0} WHERE stream_name = ANY($1))", CONSUMER_LEASES_TABLE_NAME).as_str(),
        &[streams],
        )
        .await?;

        Ok(row.get(0))
    }
}
