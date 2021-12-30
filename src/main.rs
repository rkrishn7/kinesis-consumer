mod aws;
mod connection_manager;
mod consumer;
mod kinesis_butler;
mod proto;
mod server;
mod storage;

use sqlx::postgres::PgPoolOptions;
use tonic::transport::Server;

use storage::postgres::PostgresKinesisStorageBackend;

use kinesis_butler::KinesisButler;
use proto::consumer_service_server::ConsumerServiceServer;

use connection_manager::MemoryConnectionManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    // Postgres storage backend initialization
    // TODO: add check when more storage backends are added
    let mut postgres_storage_backend = PostgresKinesisStorageBackend::new(
        std::env::var("POSTGRES_DATABASE_URL").expect(
            "
        Expected environment variable `POSTGRES_DATABASE_URL` 
        when using value postgres as a storage backend",
        ),
    );
    postgres_storage_backend
        .init(PgPoolOptions::new())
        .await
        .expect("Unable to initialize PostgreSQL storage backend");
    let kinesis_client = aws::kinesis::create_client();
    let connection_manager = MemoryConnectionManager::new();
    let consumer_service = KinesisButler::new(
        postgres_storage_backend,
        kinesis_client,
        connection_manager,
    );

    Server::builder()
        .add_service(ConsumerServiceServer::new(consumer_service))
        .serve(addr)
        .await?;

    Ok(())
}
