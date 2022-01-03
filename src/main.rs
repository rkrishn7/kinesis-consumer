mod aws;
mod consumer_lease;
mod kinesis_butler;
mod proto;
mod record_processor;
mod server;
mod storage;

use sqlx::postgres::PgPoolOptions;
use tonic::transport::Server;

use storage::postgres::PostgresKinesisStorageBackend;

use kinesis_butler::KinesisButler;
use proto::consumer_service_server::ConsumerServiceServer;

use record_processor::MemoryRecordProcessorRegister;

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
    let record_processor_register = MemoryRecordProcessorRegister::new();
    let kinesis_butler = KinesisButler::new(
        postgres_storage_backend,
        kinesis_client,
        record_processor_register,
    );

    kinesis_butler.listen_shutdown();

    Server::builder()
        .add_service(ConsumerServiceServer::new(kinesis_butler))
        .serve(addr)
        .await?;

    Ok(())
}
