use super::KinesisStorageBackend;
use crate::consumer_lease::ConsumerLease;
use async_trait::async_trait;

use sqlx::postgres::PgPool;
use sqlx::postgres::PgPoolOptions;
use sqlx::Executor;
use tokio::runtime::Handle;

const CONSUMER_LEASES_TABLE_NAME: &'static str =
    "kinesis_butler_consumer_leases";

#[derive(Clone)]
pub struct PostgresKinesisStorageBackend {
    pool: Option<PgPool>,
    connection_uri: String,
    instance_id: uuid::Uuid,
}

impl PostgresKinesisStorageBackend {
    pub fn new(connection_uri: String) -> Self {
        Self {
            pool: None,
            connection_uri,
            instance_id: uuid::Uuid::new_v4(),
        }
    }

    fn pool(&self) -> &PgPool {
        self.pool.as_ref().unwrap()
    }

    async fn create_leases_table(&self) -> sqlx::Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ( 
                id                  SERIAL NOT NULL PRIMARY KEY,
                consumer_arn        VARCHAR(255) NOT NULL,
                shard_id            VARCHAR(255) NOT NULL,
                stream_name         VARCHAR(255) NOT NULL,
                app_name            VARCHAR(255) NOT NULL,
                instance_id         UUID DEFAULT NULL,
                last_processed_sn   VARCHAR(255) DEFAULT NULL,
                UNIQUE (shard_id, stream_name, app_name)
            )",
            CONSUMER_LEASES_TABLE_NAME
        );

        let mut conn = self.pool().acquire().await?;

        conn.execute(sql.as_str()).await.map(|_| ())
    }

    pub async fn init(&mut self, options: PgPoolOptions) -> sqlx::Result<()> {
        self.pool = Some(options.connect(self.connection_uri.as_str()).await?);
        self.create_leases_table().await
    }
}

#[async_trait]
impl KinesisStorageBackend for PostgresKinesisStorageBackend {
    type Error = sqlx::Error;

    async fn checkpoint_lease(
        &self,
        lease: &ConsumerLease,
        last_processed_sn: &str,
    ) -> Result<(), Self::Error> {
        let sql = format!(
            "UPDATE {}
            SET
                last_processed_sn = $1
            WHERE
                consumer_arn = $2
            AND
                shard_id = $3
            AND
                stream_name = $4
            AND
                app_name = $5
            ",
            CONSUMER_LEASES_TABLE_NAME
        );

        sqlx::query(sql.as_str())
            .bind(last_processed_sn)
            .bind(lease.consumer_arn())
            .bind(lease.shard_id())
            .bind(lease.stream_name())
            .bind(lease.app_name())
            .execute(self.pool())
            .await
            .map(|_| ())
    }

    async fn claim_available_leases(
        &self,
        limit: i64,
        stream_name: &str,
        app_name: &str,
    ) -> Result<Vec<ConsumerLease>, Self::Error> {
        let sql = format!(
            "UPDATE {0}
            SET
                instance_id = $1
            WHERE
                id IN (
                        SELECT id
                        FROM {0}
                        WHERE
                            stream_name = $2
                        AND
                            instance_id IS NULL
                        AND
                            app_name = $3
                        LIMIT $4
                    )
            RETURNING *",
            CONSUMER_LEASES_TABLE_NAME
        );

        sqlx::query_as::<_, ConsumerLease>(sql.as_str())
            .bind(&self.instance_id)
            .bind(stream_name)
            .bind(app_name)
            .bind(limit)
            .fetch_all(self.pool())
            .await
    }

    async fn create_lease_if_not_exists(
        &self,
        consumer_arn: &str,
        stream_name: &str,
        shard_id: &str,
        app_name: &str,
    ) -> Result<(), Self::Error> {
        let sql = format!(
            "INSERT INTO {} (
                consumer_arn,
                shard_id,
                stream_name,
                app_name
            ) VALUES (
                $1,
                $2,
                $3, 
                $4
            ) ON CONFLICT DO NOTHING",
            CONSUMER_LEASES_TABLE_NAME,
        );

        sqlx::query(sql.as_str())
            .bind(consumer_arn)
            .bind(shard_id)
            .bind(stream_name)
            .bind(app_name)
            .execute(self.pool())
            .await
            .map(|_| ())
    }

    async fn release_lease(
        &self,
        lease: &ConsumerLease,
    ) -> Result<(), Self::Error> {
        let sql = format!(
            "UPDATE {}
            SET
                instance_id = NULL
            WHERE
                consumer_arn = $1
            AND
                shard_id = $2
            AND
                stream_name = $3
            AND
                app_name = $4",
            CONSUMER_LEASES_TABLE_NAME
        );

        sqlx::query(sql.as_str())
            .bind(lease.consumer_arn())
            .bind(lease.shard_id())
            .bind(lease.stream_name())
            .bind(lease.app_name())
            .execute(self.pool())
            .await
            .map(|_| ())
    }

    async fn release_claimed_leases(&self) -> Result<(), Self::Error> {
        let sql = format!(
            "UPDATE {}
            SET
                instance_id = NULL
            WHERE
                instance_id = $1",
            CONSUMER_LEASES_TABLE_NAME
        );

        sqlx::query(sql.as_str())
            .bind(&self.instance_id)
            .execute(self.pool())
            .await
            .map(|_| ())
    }

    async fn get_lease_count(
        &self,
        stream_name: &str,
        app_name: &str,
    ) -> Result<i64, Self::Error> {
        let sql = format!(
            "SELECT
                COUNT(*)
            FROM {0}
            WHERE
                stream_name = $1
            AND
                app_name = $2",
            CONSUMER_LEASES_TABLE_NAME
        );

        let count: (i64,) = sqlx::query_as(sql.as_str())
            .bind(stream_name)
            .bind(app_name)
            .fetch_one(self.pool())
            .await?;

        Ok(count.0)
    }
}
