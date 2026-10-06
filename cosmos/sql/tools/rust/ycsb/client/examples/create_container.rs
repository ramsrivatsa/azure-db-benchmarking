//! Creates the database and `/id`-partitioned container the benchmark expects.
//!
//! ```text
//! cargo run --release --example create_container -- <uri> <key> [database] [container] [throughput]
//! ```
//!
//! Defaults: database `ycsb`, container `usertable`, 400 RU/s. Existing resources are kept.

use anyhow::{Context, Result};
use azure_data_cosmos::models::{ContainerProperties, ThroughputProperties};
use azure_data_cosmos::options::{
    BinaryEncodingOptions, ConnectionPoolOptionsBuilder, CreateContainerOptions, ServerCertificateValidation,
};
use azure_data_cosmos::{AccountReference, CosmosClient, CosmosRuntime, RoutingStrategy};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let uri = args
        .first()
        .context("usage: create_container <uri> <key> [database] [container] [throughput]")?;
    let key = args.get(1).context("missing account key")?;
    let database = args.get(2).map_or("ycsb", String::as_str);
    let container = args.get(3).map_or("usertable", String::as_str).to_string();
    let throughput: u64 = args
        .get(4)
        .map_or(Ok(400), |t| t.parse())
        .context("throughput must be an integer")?;

    let runtime = CosmosRuntime::builder()
        .with_connection_pool(
            ConnectionPoolOptionsBuilder::new()
                .with_server_certificate_validation(ServerCertificateValidation::RequiredUnlessEmulator)
                .build()?,
        )
        .build()
        .await?;
    let client = CosmosClient::builder()
        .with_runtime(runtime)
        .with_binary_encoding_options(BinaryEncodingOptions::new().with_enabled(false))
        .build(
            AccountReference::with_authentication_key(uri.parse()?, key.clone()),
            RoutingStrategy::PreferredRegions(Vec::new()),
        )
        .await?;

    match client.create_database(database, None).await {
        Ok(_) => println!("created database {database}"),
        Err(e) if e.status().is_conflict() => println!("database {database} already exists"),
        Err(e) => return Err(e).context("creating the database"),
    }
    let options = CreateContainerOptions::default().with_throughput(ThroughputProperties::manual(throughput));
    match client
        .database_client(database)
        .create_container(ContainerProperties::new(container.clone(), "/id".into()), Some(options))
        .await
    {
        Ok(_) => println!("created container {container} (partition key /id, {throughput} RU/s)"),
        Err(e) if e.status().is_conflict() => println!("container {container} already exists"),
        Err(e) => return Err(e).context("creating the container"),
    }
    Ok(())
}
