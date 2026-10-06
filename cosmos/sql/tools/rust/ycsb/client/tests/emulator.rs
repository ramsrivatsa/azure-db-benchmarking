//! Integration tests against a Cosmos DB endpoint, normally the local emulator:
//!
//! ```text
//! docker run -d -p 8081:8081 mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview
//! COSMOS_EMULATOR_ENDPOINT=http://localhost:8081 cargo test --test emulator -- --test-threads=1
//! ```
//!
//! `COSMOS_EMULATOR_KEY` overrides the emulator's well-known key. Without
//! `COSMOS_EMULATOR_ENDPOINT` every test returns early. Each test works in its own
//! database, which it deletes afterwards.

use std::collections::HashSet;
use std::io::Write;
use std::process::Command;

use azure_data_cosmos::models::ContainerProperties;
use azure_data_cosmos::options::{BinaryEncodingOptions, ConnectionPoolOptionsBuilder, ServerCertificateValidation};
use azure_data_cosmos::{AccountReference, CosmosClient, CosmosRuntime, RoutingStrategy};
use ycsb_cosmos::bindings::cosmos::CosmosDb;
use ycsb_cosmos::db::{Db, Record, Values};
use ycsb_cosmos::props::Properties;
use ycsb_cosmos::status::Status;

const WELL_KNOWN_EMULATOR_KEY: &str =
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
const TABLE: &str = "usertable";

struct Emulator {
    endpoint: String,
    key: String,
}

fn emulator() -> Option<Emulator> {
    let endpoint = std::env::var("COSMOS_EMULATOR_ENDPOINT").ok()?;
    let key = std::env::var("COSMOS_EMULATOR_KEY").unwrap_or_else(|_| WELL_KNOWN_EMULATOR_KEY.to_string());
    Some(Emulator { endpoint, key })
}

macro_rules! require_emulator {
    () => {
        match emulator() {
            Some(e) => e,
            None => {
                eprintln!("skipping: COSMOS_EMULATOR_ENDPOINT is not set");
                return;
            }
        }
    };
}

/// A database (with an `/id`-partitioned `usertable`) that is deleted on drop.
struct TestDatabase {
    client: CosmosClient,
    name: String,
}

impl TestDatabase {
    async fn create(emulator: &Emulator) -> Self {
        let runtime = CosmosRuntime::builder()
            .with_connection_pool(
                ConnectionPoolOptionsBuilder::new()
                    .with_server_certificate_validation(ServerCertificateValidation::RequiredUnlessEmulator)
                    .build()
                    .unwrap(),
            )
            .build()
            .await
            .unwrap();
        let client = CosmosClient::builder()
            .with_runtime(runtime)
            .with_binary_encoding_options(BinaryEncodingOptions::new().with_enabled(false))
            .build(
                AccountReference::with_authentication_key(emulator.endpoint.parse().unwrap(), emulator.key.clone()),
                RoutingStrategy::PreferredRegions(Vec::new()),
            )
            .await
            .unwrap();
        let name = format!("ycsbtest{:016x}", ycsb_cosmos::rng::next_u64());
        client.create_database(&name, None).await.expect("create test database");
        client
            .database_client(name.as_str())
            .create_container(ContainerProperties::new(TABLE, "/id".into()), None)
            .await
            .expect("create test container");
        Self { client, name }
    }

    fn properties(&self, emulator: &Emulator, extra: &str) -> Properties {
        let mut p = Properties::new();
        p.load_str(&format!(
            "azurecosmos.uri = {}\nazurecosmos.primaryKey = {}\nazurecosmos.databaseName = {}\n{extra}",
            emulator.endpoint, emulator.key, self.name
        ));
        p
    }

    async fn binding(&self, emulator: &Emulator, extra: &str) -> CosmosDb {
        let db = CosmosDb::new(&self.properties(emulator, extra)).unwrap();
        db.init().await.expect("binding init");
        db
    }

    async fn delete(self) {
        let _ = self.client.database_client(self.name.as_str()).delete(None).await;
    }
}

fn values(pairs: &[(&str, &str)]) -> Values {
    pairs.iter().map(|(f, v)| (f.to_string(), v.to_string())).collect()
}

fn fields(names: &[&str]) -> HashSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn insert_read_update_delete_round_trip() {
    let emulator = require_emulator!();
    let database = TestDatabase::create(&emulator).await;
    let db = database.binding(&emulator, "").await;

    assert_eq!(
        db.insert(TABLE, "user1", &values(&[("field0", "a\"b\\c"), ("field1", "one")]))
            .await,
        Status::Ok
    );

    let mut all = Record::new();
    assert_eq!(db.read(TABLE, "user1", None, &mut all).await, Status::Ok);
    assert_eq!(all["id"], "user1");
    assert_eq!(all["field0"], "a\"b\\c");
    assert_eq!(all["field1"], "one");

    let mut some = Record::new();
    assert_eq!(
        db.read(TABLE, "user1", Some(&fields(&["field1"])), &mut some).await,
        Status::Ok
    );
    assert_eq!(some.len(), 1);
    assert_eq!(some["field1"], "one");

    // Replace-mode update keeps the fields it does not touch.
    assert_eq!(
        db.update(TABLE, "user1", &values(&[("field1", "two")])).await,
        Status::Ok
    );
    let mut after = Record::new();
    db.read(TABLE, "user1", None, &mut after).await;
    assert_eq!(after["field1"], "two");
    assert_eq!(after["field0"], "a\"b\\c");

    assert_eq!(db.delete(TABLE, "user1").await, Status::Ok);
    assert_eq!(
        db.read(TABLE, "user1", None, &mut Record::new()).await,
        Status::NotFound
    );
    assert_eq!(db.delete(TABLE, "user1").await, Status::Error);
    database.delete().await;
}

#[tokio::test]
async fn insert_conflicts_unless_upsert_is_enabled() {
    let emulator = require_emulator!();
    let database = TestDatabase::create(&emulator).await;
    let create = database.binding(&emulator, "").await;
    assert_eq!(
        create.insert(TABLE, "dup", &values(&[("field0", "x")])).await,
        Status::Ok
    );
    assert_eq!(
        create.insert(TABLE, "dup", &values(&[("field0", "y")])).await,
        Status::Error
    );

    let upsert = database.binding(&emulator, "azurecosmos.useUpsert = true\n").await;
    assert_eq!(
        upsert.insert(TABLE, "dup", &values(&[("field0", "z")])).await,
        Status::Ok
    );
    let mut record = Record::new();
    upsert.read(TABLE, "dup", None, &mut record).await;
    assert_eq!(record["field0"], "z");
    database.delete().await;
}

#[tokio::test]
async fn update_of_missing_item_fails_in_both_modes() {
    let emulator = require_emulator!();
    let database = TestDatabase::create(&emulator).await;
    for mode in ["replace", "patch"] {
        let db = database
            .binding(&emulator, &format!("azurecosmos.updateMode = {mode}\n"))
            .await;
        assert_eq!(
            db.update(TABLE, "missing", &values(&[("field0", "x")])).await,
            Status::Error,
            "{mode}"
        );
    }
    database.delete().await;
}

#[tokio::test]
async fn patch_mode_update_replaces_fields() {
    let emulator = require_emulator!();
    let database = TestDatabase::create(&emulator).await;
    let db = database.binding(&emulator, "azurecosmos.updateMode = patch\n").await;
    db.insert(TABLE, "p1", &values(&[("field0", "a"), ("field1", "b")]))
        .await;
    let status = db.update(TABLE, "p1", &values(&[("field1", "patched")])).await;
    assert_eq!(status, Status::Ok, "server-side PATCH should succeed");
    let mut record = Record::new();
    db.read(TABLE, "p1", None, &mut record).await;
    assert_eq!(record["field1"], "patched");
    assert_eq!(record["field0"], "a");
    database.delete().await;
}

#[tokio::test]
async fn scan_returns_at_most_count_records_from_start_key() {
    let emulator = require_emulator!();
    let database = TestDatabase::create(&emulator).await;
    let db = database.binding(&emulator, "").await;
    for i in 0..20 {
        let key = format!("user{i:02}");
        assert_eq!(
            db.insert(TABLE, &key, &values(&[("field0", "v0"), ("field1", "v1")]))
                .await,
            Status::Ok
        );
    }

    let mut rows = Vec::new();
    assert_eq!(db.scan(TABLE, "user10", 5, None, &mut rows).await, Status::Ok);
    assert_eq!(rows.len(), 5);
    assert!(rows.iter().all(|r| r["id"].as_str() >= "user10"), "{rows:?}");

    let mut projected = Vec::new();
    assert_eq!(
        db.scan(TABLE, "user15", 100, Some(&fields(&["field1"])), &mut projected)
            .await,
        Status::Ok
    );
    assert_eq!(projected.len(), 5, "user15..user19");
    assert!(
        projected.iter().all(|r| r.len() == 1 && r["field1"] == "v1"),
        "{projected:?}"
    );
    database.delete().await;
}

#[tokio::test]
async fn init_reports_missing_database_and_bad_key() {
    let emulator = require_emulator!();
    let mut p = Properties::new();
    p.load_str(&format!(
        "azurecosmos.uri = {}\nazurecosmos.primaryKey = {}\nazurecosmos.databaseName = doesnotexist{:x}\n",
        emulator.endpoint,
        emulator.key,
        ycsb_cosmos::rng::next_u64()
    ));
    let err = CosmosDb::new(&p).unwrap().init().await.unwrap_err().to_string();
    assert!(err.contains("Invalid database name"), "{err}");

    let database = TestDatabase::create(&emulator).await;
    let mut bad = database.properties(&emulator, "");
    // A syntactically valid key that is not the account's key.
    bad.set(
        "azurecosmos.primaryKey",
        "dGhpcyBpcyBub3QgdGhlIGVtdWxhdG9yIGtleSBhdCBhbGwgYXQgYWxsIGF0IGFsbCE=",
    );
    let result = CosmosDb::new(&bad).unwrap().init().await;
    database.delete().await;
    // The vNext emulator may not validate keys; a real account rejects the request.
    if let Err(e) = result {
        assert!(e.to_string().contains("Invalid database name"), "{e}");
    }
}

/// Runs the real `ycsb` binary for a load and a run phase and checks the YCSB output.
#[tokio::test]
async fn cli_load_and_run_against_emulator() {
    let emulator = require_emulator!();
    let database = TestDatabase::create(&emulator).await;
    let mut props_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(
        props_file,
        "azurecosmos.uri = {}\nazurecosmos.primaryKey = {}\nazurecosmos.databaseName = {}\n",
        emulator.endpoint, emulator.key, database.name
    )
    .unwrap();
    let workloads = concat!(env!("CARGO_MANIFEST_DIR"), "/workloads");
    let ycsb = env!("CARGO_BIN_EXE_ycsb");

    let load = Command::new(ycsb)
        .args(["load", "azurecosmos", "-P", &format!("{workloads}/workloada"), "-P"])
        .arg(props_file.path())
        .args(["-p", "recordcount=200", "-s", "-threads", "4"])
        .output()
        .unwrap();
    let load_out = String::from_utf8_lossy(&load.stdout);
    assert!(
        load.status.success(),
        "{load_out}\n{}",
        String::from_utf8_lossy(&load.stderr)
    );
    assert!(load_out.contains("[INSERT], Return=OK, 200\n"), "{load_out}");

    let run = Command::new(ycsb)
        .args(["run", "azurecosmos", "-P", &format!("{workloads}/workloada"), "-P"])
        .arg(props_file.path())
        .args([
            "-p",
            "recordcount=200",
            "-p",
            "operationcount=400",
            "-s",
            "-threads",
            "4",
            "-target",
            "400",
        ])
        .output()
        .unwrap();
    let run_out = String::from_utf8_lossy(&run.stdout);
    let run_err = String::from_utf8_lossy(&run.stderr);
    assert!(run.status.success(), "{run_out}\n{run_err}");
    let ok_count = |op: &str| -> u64 {
        run_out
            .lines()
            .find_map(|l| l.strip_prefix(&format!("[{op}], Return=OK, ")))
            .map_or(0, |n| n.parse().unwrap())
    };
    assert_eq!(ok_count("READ") + ok_count("UPDATE"), 400, "{run_out}");
    assert!(!run_out.contains("-FAILED]"), "{run_out}\n{run_err}");
    assert!(run_err.contains("current ops/sec"), "status lines missing: {run_err}");
    database.delete().await;
}
