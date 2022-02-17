use sqlx::postgres::PgPoolOptions;
use tonic::transport::Server;

use kinesis_butler::storage::postgres::PostgresKinesisStorageBackend;

use kinesis_butler::proto::consumer_service_server::ConsumerServiceServer;
use kinesis_butler::KinesisButler;

use kinesis_butler::connection_table::MemoryConnectionTable;

use clap::{ArgEnum, Parser};

use kinesis_butler::storage::KinesisStorageBackend;

#[derive(Parser)]
#[clap(name = "kinesis-butler")]
#[clap(author = "Rohan Krishnaswamy <rkrishn7@ucsc.edu>")]
#[clap(version = "0.1.0")]
#[clap(about = "A utility to consume data from Amazon Kinesis Data Streams", long_about = None)]
struct Args {
    #[clap(arg_enum)]
    storage_backend: SupportedStorageBackend,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
enum SupportedStorageBackend {
    Postgres,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut storage_backend;

    match args.storage_backend {
        SupportedStorageBackend::Postgres => {
            storage_backend = PostgresKinesisStorageBackend::new(
                std::env::var("POSTGRES_DATABASE_URL").expect(
                    "
                Expected environment variable `POSTGRES_DATABASE_URL` 
                when using value postgres as a storage backend",
                ),
            );

            storage_backend.init(PgPoolOptions::new()).await?;
        }
    }

    let addr = "[::1]:50051".parse()?;

    let kinesis_client = kinesis_butler::aws::kinesis::create_client();
    let connection_table = MemoryConnectionTable::new();
    let kinesis_butler = KinesisButler::new(
        storage_backend.clone(),
        kinesis_client,
        connection_table,
    );

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            if let Err(_) = storage_backend.release_claimed_leases().await {
                eprintln!("couldn't release leases");
            }
        },
        _ = Server::builder()
        .add_service(ConsumerServiceServer::new(kinesis_butler))
        .serve(addr) => (),
    }

    Ok(())
}
