//! Port of YCSB's Azure Cosmos DB (NoSQL API) binding, `site.ycsb.db.AzureCosmosClient`,
//! on top of the Rust SDK (`azure_data_cosmos`).
//!
//! Each record is one item whose `id` and partition key are both the YCSB key (the
//! container must be partitioned on `/id`), with one string property per field. All the
//! `azurecosmos.*` properties of the Java binding are accepted; see `conf/azurecosmos.properties`
//! for how each maps onto the Rust SDK.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use azure_data_cosmos::diagnostics::DiagnosticsContext;
use azure_data_cosmos::models::{PatchInstructions, PatchOperation};
use azure_data_cosmos::options::{
    BinaryEncodingOptions, ConnectionPoolOptionsBuilder, FeedOptions, ItemWriteOptions, MaxItemCountHint,
    OperationOptionsBuilder, PatchItemOptions, PatchStrategy, Precondition, QueryOptions, ReadConsistencyStrategy,
    Region, ServerCertificateValidation, ThrottlingRetryOptionsBuilder, UserAgentSuffix,
};
use azure_data_cosmos::{
    AccountEndpoint, AccountReference, ContainerClient, CosmosClient, CosmosError, CosmosRuntime, FeedScope, Query,
    RoutingStrategy,
};
use futures::StreamExt;
use serde_json::{Map, Value};
use tokio::sync::OnceCell;
use tracing::{error, info, warn};

use crate::db::{Db, Record, Values};
use crate::props::Properties;
use crate::status::Status;

/// Attempts of the read + conditional-replace update before giving up (Java: `NUM_UPDATE_ATTEMPTS`).
pub const NUM_UPDATE_ATTEMPTS: usize = 4;
const DEFAULT_USER_AGENT: &str = "azurecosmos-ycsb";

/// `azurecosmos.consistencyLevel`, named like Java's `ConsistencyLevel` enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsistencyLevel {
    Strong,
    BoundedStaleness,
    Session,
    ConsistentPrefix,
    Eventual,
}

impl ConsistencyLevel {
    /// `ConsistencyLevel.valueOf`: exact, case-sensitive enum names.
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "STRONG" => Self::Strong,
            "BOUNDED_STALENESS" => Self::BoundedStaleness,
            "SESSION" => Self::Session,
            "CONSISTENT_PREFIX" => Self::ConsistentPrefix,
            "EVENTUAL" => Self::Eventual,
            _ => return None,
        })
    }

    /// The Rust SDK replaced client consistency levels with read consistency strategies.
    /// Bounded staleness reads are region-local quorum reads, which is what
    /// `LatestCommitted` does; consistent prefix reads are single-replica reads like
    /// eventual ones.
    pub fn read_strategy(self) -> ReadConsistencyStrategy {
        match self {
            Self::Strong => ReadConsistencyStrategy::GlobalStrong,
            Self::BoundedStaleness => ReadConsistencyStrategy::LatestCommitted,
            Self::Session => ReadConsistencyStrategy::Session,
            Self::ConsistentPrefix | Self::Eventual => ReadConsistencyStrategy::Eventual,
        }
    }
}

impl fmt::Display for ConsistencyLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Strong => "STRONG",
            Self::BoundedStaleness => "BOUNDED_STALENESS",
            Self::Session => "SESSION",
            Self::ConsistentPrefix => "CONSISTENT_PREFIX",
            Self::Eventual => "EVENTUAL",
        })
    }
}

/// How `update` modifies an item (`azurecosmos.updateMode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpdateMode {
    /// Read the item, then replace it conditioned on the read ETag, retrying up to
    /// [`NUM_UPDATE_ATTEMPTS`] times (brianfrankcooper/YCSB binding).
    Replace,
    /// One server-side PATCH with a `replace` operation per field (Azure/YCSB binding).
    Patch,
}

/// The binding's configuration, read from `azurecosmos.*` properties.
#[derive(Clone, Debug)]
pub struct CosmosConfig {
    pub uri: String,
    pub primary_key: String,
    pub database_name: String,
    pub use_upsert: bool,
    pub include_exception_stack_in_log: bool,
    pub user_agent: String,
    pub use_gateway: bool,
    pub consistency_level: ConsistencyLevel,
    pub max_retry_attempts_on_throttled_requests: i32,
    pub max_retry_wait_time_in_seconds: i32,
    pub gateway_max_connection_pool_size: i32,
    pub gateway_idle_connection_timeout_in_seconds: i32,
    pub direct_max_connections_per_endpoint: i32,
    pub direct_idle_connection_timeout_in_seconds: i32,
    pub max_degree_of_parallelism: i32,
    pub max_buffered_item_count: i32,
    pub preferred_page_size: i32,
    pub diagnostics_latency_threshold_in_ms: i32,
    pub preferred_regions: Vec<String>,
    pub app_insight_connection_string: Option<String>,
    pub update_mode: UpdateMode,
    pub binary_encoding: Option<bool>,
    pub session_capturing_disabled: bool,
}

impl CosmosConfig {
    pub fn from_properties(p: &Properties) -> Result<Self> {
        let primary_key = p.get("azurecosmos.primaryKey").unwrap_or_default().to_string();
        if primary_key.is_empty() {
            bail!("Missing primary key required to connect to the database.");
        }
        let uri = p.get("azurecosmos.uri").unwrap_or_default().to_string();
        if uri.is_empty() {
            bail!("Missing uri required to connect to the database.");
        }
        let consistency = p.get_or("azurecosmos.consistencyLevel", "SESSION");
        let consistency_level = ConsistencyLevel::parse(consistency)
            .ok_or_else(|| anyhow!("No enum constant com.azure.cosmos.ConsistencyLevel.{consistency}"))?;
        let update_mode = match p
            .get_or("azurecosmos.updateMode", "replace")
            .to_ascii_lowercase()
            .as_str()
        {
            "replace" => UpdateMode::Replace,
            "patch" => UpdateMode::Patch,
            other => bail!("azurecosmos.updateMode must be 'replace' or 'patch', got '{other}'"),
        };
        let preferred_regions = p
            .get("azurecosmos.preferredRegionList")
            .map(|list| {
                list.trim()
                    .split(',')
                    .map(str::trim)
                    .filter(|r| !r.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            uri,
            primary_key,
            database_name: p.get_or("azurecosmos.databaseName", "ycsb").to_string(),
            use_upsert: p.parse_bool("azurecosmos.useUpsert", false),
            include_exception_stack_in_log: p.parse_bool("azurecosmos.includeExceptionStackInLog", false),
            user_agent: p.get_or("azurecosmos.userAgent", DEFAULT_USER_AGENT).to_string(),
            use_gateway: p.parse_bool("azurecosmos.useGateway", false),
            consistency_level,
            max_retry_attempts_on_throttled_requests: p
                .int_or_default("azurecosmos.maxRetryAttemptsOnThrottledRequests", -1),
            max_retry_wait_time_in_seconds: p.int_or_default("azurecosmos.maxRetryWaitTimeInSeconds", -1),
            gateway_max_connection_pool_size: p.int_or_default("azurecosmos.gatewayMaxConnectionPoolSize", -1),
            gateway_idle_connection_timeout_in_seconds: p
                .int_or_default("azurecosmos.gatewayIdleConnectionTimeoutInSeconds", -1),
            direct_max_connections_per_endpoint: p.int_or_default("azurecosmos.directMaxConnectionsPerEndpoint", -1),
            direct_idle_connection_timeout_in_seconds: p
                .int_or_default("azurecosmos.directIdleConnectionTimeoutInSeconds", -1),
            max_degree_of_parallelism: p.int_or_default("azurecosmos.maxDegreeOfParallelism", -1),
            max_buffered_item_count: p.int_or_default("azurecosmos.maxBufferedItemCount", 0),
            preferred_page_size: p.int_or_default("azurecosmos.preferredPageSize", -1),
            diagnostics_latency_threshold_in_ms: p.int_or_default("azurecosmos.diagnosticsLatencyThresholdInMS", -1),
            preferred_regions,
            app_insight_connection_string: p
                .get("azurecosmos.appInsightConnectionString")
                .filter(|s| !s.trim().is_empty())
                .map(str::to_string),
            update_mode,
            binary_encoding: p
                .get("azurecosmos.binaryEncoding")
                .map(|v| v.eq_ignore_ascii_case("true")),
            session_capturing_disabled: p.parse_bool("azurecosmos.sessionCapturingDisabled", false),
        })
    }
}

/// A per-task handle; all handles share one SDK client, like the Java binding's static client.
#[derive(Clone)]
pub struct CosmosDb {
    shared: Arc<Shared>,
}

struct Shared {
    config: CosmosConfig,
    /// Initialized by the first task's `init`. A failure is cached so the remaining tasks
    /// fail fast instead of each repeating a doomed connection attempt.
    connection: OnceCell<std::result::Result<Connection, String>>,
    transport_logged: AtomicBool,
}

struct Connection {
    client: CosmosClient,
    /// Resolving a container loads its partition topology, so each table is resolved once
    /// even when many tasks ask for it at the same time.
    containers: RwLock<HashMap<String, Arc<OnceCell<Arc<ContainerClient>>>>>,
}

impl CosmosDb {
    pub fn new(props: &Properties) -> Result<Self> {
        Ok(Self::from_config(CosmosConfig::from_properties(props)?))
    }

    pub fn from_config(config: CosmosConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                config,
                connection: OnceCell::new(),
                transport_logged: AtomicBool::new(false),
            }),
        }
    }

    fn config(&self) -> &CosmosConfig {
        &self.shared.config
    }

    fn connection(&self) -> std::result::Result<&Connection, String> {
        match self.shared.connection.get() {
            Some(Ok(connection)) => Ok(connection),
            Some(Err(e)) => Err(e.clone()),
            None => Err("the Cosmos DB client is not initialized".to_string()),
        }
    }

    async fn container(&self, table: &str) -> std::result::Result<Arc<ContainerClient>, String> {
        let connection = self.connection()?;
        let cached = connection
            .containers
            .read()
            .expect("container cache lock poisoned")
            .get(table)
            .cloned();
        let cell = match cached {
            Some(cell) => cell,
            None => Arc::clone(
                connection
                    .containers
                    .write()
                    .expect("container cache lock poisoned")
                    .entry(table.to_string())
                    .or_default(),
            ),
        };
        // A failed resolution leaves the cell empty, so a later operation retries it.
        let container = cell
            .get_or_try_init(|| async {
                connection
                    .client
                    .database_client(self.config().database_name.as_str())
                    .container_client(table, None)
                    .await
                    .map(Arc::new)
                    .map_err(|e| self.describe_error(&e))
            })
            .await?;
        Ok(Arc::clone(container))
    }

    fn describe_error(&self, e: &CosmosError) -> String {
        let status = u16::from(e.status().status_code());
        if self.config().include_exception_stack_in_log {
            format!("statusCode {status}: {e:?}")
        } else {
            format!("statusCode {status}: {e}")
        }
    }

    /// Logs the transport of the first successful request, and diagnostics for slow operations.
    fn after_response(&self, operation: &str, started: Instant, diagnostics: &DiagnosticsContext) {
        if !self.shared.transport_logged.load(Ordering::Relaxed) {
            if let Some(request) = diagnostics.requests().last() {
                if !self.shared.transport_logged.swap(true, Ordering::Relaxed) {
                    info!(
                        "Data-plane transport: {} ({:?}) to {} in region {}",
                        request.transport_kind().as_str(),
                        request.transport_http_version(),
                        request.endpoint(),
                        request.region().map_or("unknown", |r| r.as_str())
                    );
                }
            }
        }
        let threshold = self.config().diagnostics_latency_threshold_in_ms;
        if threshold > 0 && started.elapsed() > Duration::from_millis(threshold as u64) {
            warn!("{operation} diagnostics: {diagnostics}");
        }
    }

    /// Reads an item, returning it with an If-Match precondition on its current ETag.
    async fn read_document(
        &self,
        container: &ContainerClient,
        key: &str,
    ) -> std::result::Result<(Map<String, Value>, Option<Precondition>), String> {
        let started = Instant::now();
        let response = container
            .read_item(key.to_string(), key, None)
            .await
            .map_err(|e| self.describe_error(&e))?;
        self.after_response("READ", started, &response.diagnostics());
        let if_match = response.headers().etag().cloned().map(Precondition::if_match);
        let document = response
            .into_model::<Map<String, Value>>()
            .map_err(|e| format!("could not decode item: {e}"))?;
        Ok((document, if_match))
    }

    async fn update_with_replace(&self, table: &str, key: &str, values: &Values) -> Status {
        for attempt in 0..NUM_UPDATE_ATTEMPTS {
            let outcome = async {
                let container = self.container(table).await?;
                let (mut document, if_match) = self.read_document(&container, key).await?;
                for (field, value) in values {
                    document.insert(field.clone(), Value::String(value.clone()));
                }
                let mut options = ItemWriteOptions::default();
                if let Some(if_match) = if_match {
                    options = options.with_precondition(if_match);
                }
                let started = Instant::now();
                let response = container
                    .replace_item(key.to_string(), key, document, Some(options))
                    .await
                    .map_err(|e| self.describe_error(&e))?;
                self.after_response("REPLACE", started, &response.diagnostics());
                Ok::<(), String>(())
            }
            .await;
            match outcome {
                Ok(()) => return Status::Ok,
                Err(e) => error!(
                    "Failed to update key {key} to collection {table} in database {} on attempt {attempt} {e}",
                    self.config().database_name
                ),
            }
        }
        Status::Error
    }

    async fn update_with_patch(&self, table: &str, key: &str, values: &Values) -> Status {
        let outcome = async {
            let container = self.container(table).await?;
            let operations: Vec<PatchOperation> = values
                .iter()
                .map(|(field, value)| PatchOperation::replace(format!("/{field}"), Value::String(value.clone())))
                .collect();
            let options = PatchItemOptions::default().with_strategy(PatchStrategy::ServerSide);
            let started = Instant::now();
            let response = container
                .patch_item(key.to_string(), key, PatchInstructions::from(operations), Some(options))
                .await
                .map_err(|e| self.describe_error(&e))?;
            self.after_response("PATCH", started, &response.diagnostics());
            Ok::<(), String>(())
        }
        .await;
        match outcome {
            Ok(()) => Status::Ok,
            Err(e) => {
                error!(
                    "Failed to update key {key} to collection {table} in database {} {e}",
                    self.config().database_name
                );
                Status::Error
            }
        }
    }
}

async fn connect(config: &CosmosConfig) -> Result<Connection> {
    let throttling = throttling_retry_options(config)?;
    info!(
        "Creating Cosmos DB client {}, useGateway={}, consistencyLevel={}, maxRetryAttemptsOnThrottledRequests={}, \
         maxRetryWaitTimeInSeconds={} useUpsert={}, maxDegreeOfParallelism={}, maxBufferedItemCount={}, preferredPageSize={}",
        config.uri,
        config.use_gateway,
        config.consistency_level,
        throttling.max_retry_count.unwrap_or_default(),
        throttling.max_retry_wait_time.unwrap_or_default().as_secs(),
        config.use_upsert,
        config.max_degree_of_parallelism,
        config.max_buffered_item_count,
        config.preferred_page_size
    );
    warn_about_unsupported_options(config);

    let endpoint: AccountEndpoint = config
        .uri
        .parse()
        .map_err(|e| anyhow!("Illegal argument passed in. Check the format of your parameters. ({e})"))?;
    let emulator = is_emulator_host(endpoint.url());

    let runtime = CosmosRuntime::builder()
        .with_connection_pool(connection_pool_options(config)?)
        .build()
        .await
        .context("creating the Cosmos DB runtime")?;

    let operation_options = OperationOptionsBuilder::new()
        .with_read_consistency_strategy(config.consistency_level.read_strategy())
        .with_throttling_retry_options(throttling)
        .with_session_capturing_disabled(config.session_capturing_disabled);

    let mut builder = CosmosClient::builder()
        .with_runtime(runtime)
        .with_default_operation_options(operation_options.build());
    match UserAgentSuffix::try_new(config.user_agent.clone()) {
        Some(suffix) => builder = builder.with_user_agent_suffix(suffix),
        None => warn!(
            "azurecosmos.userAgent '{}' is not a valid User-Agent suffix (at most 25 header-safe characters); not sending it",
            config.user_agent
        ),
    }
    // The Linux vNext emulator rejects Cosmos binary JSON, which the SDK enables by default.
    if let Some(enabled) = config.binary_encoding.or(emulator.then_some(false)) {
        builder = builder.with_binary_encoding_options(BinaryEncodingOptions::new().with_enabled(enabled));
    }

    let routing = RoutingStrategy::PreferredRegions(
        config
            .preferred_regions
            .iter()
            .map(|r| Region::new(r.clone()))
            .collect(),
    );
    let account = AccountReference::with_authentication_key(endpoint, config.primary_key.clone());
    // Building the client fetches the account properties, so connectivity and key problems
    // surface here as well as malformed settings.
    let client = builder.build(account, routing).await.map_err(|e| {
        if e.status().is_bad_request() {
            anyhow!("Illegal argument passed in. Check the format of your parameters. ({e})")
        } else {
            anyhow!("Could not connect to the Cosmos DB account at {}: {e}", config.uri)
        }
    })?;
    info!("Azure Cosmos DB connection created to {}", config.uri);

    // Verify the database exists, as the Java binding does.
    client
        .database_client(config.database_name.as_str())
        .read(None)
        .await
        .map_err(|e| {
            anyhow!(
                "Invalid database name ({}) or failed to read database. {e}",
                config.database_name
            )
        })?;

    Ok(Connection {
        client,
        containers: RwLock::new(HashMap::new()),
    })
}

fn warn_about_unsupported_options(config: &CosmosConfig) {
    if !config.use_gateway {
        info!(
            "Direct (TCP) mode is not available in the Rust SDK; requests use Gateway 2.0 when the account offers it, \
             otherwise the standard gateway. Set azurecosmos.useGateway=true to always use the standard gateway."
        );
    }
    if config.max_degree_of_parallelism != -1 {
        warn!("azurecosmos.maxDegreeOfParallelism has no equivalent in the Rust SDK and is ignored");
    }
    if config.max_buffered_item_count != 0 {
        warn!("azurecosmos.maxBufferedItemCount has no equivalent in the Rust SDK and is ignored");
    }
    if config.app_insight_connection_string.is_some() {
        warn!("azurecosmos.appInsightConnectionString is not supported by the Rust client and is ignored");
    }
    if matches!(
        config.consistency_level,
        ConsistencyLevel::BoundedStaleness | ConsistencyLevel::ConsistentPrefix
    ) {
        info!(
            "consistencyLevel={} maps to the Rust SDK read strategy {:?}",
            config.consistency_level,
            config.consistency_level.read_strategy()
        );
    }
}

fn non_negative(name: &str, value: i32) -> Result<Option<u64>> {
    match value {
        -1 => Ok(None),
        v if v < 0 => bail!("Illegal argument passed in. Check the format of your parameters. ({name}={v})"),
        v => Ok(Some(v as u64)),
    }
}

fn connection_pool_options(config: &CosmosConfig) -> Result<azure_data_cosmos::options::ConnectionPoolOptions> {
    // Relaxes certificate validation only for emulator hosts (self-signed certificate);
    // validation stays mandatory for every other endpoint.
    let mut pool = ConnectionPoolOptionsBuilder::new()
        .with_server_certificate_validation(ServerCertificateValidation::RequiredUnlessEmulator);
    if config.use_gateway {
        pool = pool.with_gateway_v2_disabled(true);
    }
    if let Some(n) = non_negative("gatewayMaxConnectionPoolSize", config.gateway_max_connection_pool_size)? {
        pool = pool.with_max_idle_connections_per_endpoint(n as usize);
    }
    if let Some(s) = non_negative(
        "gatewayIdleConnectionTimeoutInSeconds",
        config.gateway_idle_connection_timeout_in_seconds,
    )? {
        pool = pool.with_idle_connection_timeout(Duration::from_secs(s));
    }
    if let Some(n) = non_negative(
        "directMaxConnectionsPerEndpoint",
        config.direct_max_connections_per_endpoint,
    )? {
        pool = pool.with_max_http2_connections_per_endpoint(n as usize);
    }
    if let Some(s) = non_negative(
        "directIdleConnectionTimeoutInSeconds",
        config.direct_idle_connection_timeout_in_seconds,
    )? {
        pool = pool.with_idle_http2_client_timeout(Duration::from_secs(s));
    }
    pool.build()
        .map_err(|e| anyhow!("Illegal argument passed in. Check the format of your parameters. ({e})"))
}

/// Java SDK defaults for rate-limited (429) requests. The Rust SDK's own data-plane defaults
/// retry far longer, which would change how throttled runs report latency and failures.
const JAVA_MAX_RETRY_ATTEMPTS_ON_THROTTLED_REQUESTS: u32 = 9;
const JAVA_MAX_RETRY_WAIT_TIME: Duration = Duration::from_secs(30);

fn throttling_retry_options(config: &CosmosConfig) -> Result<azure_data_cosmos::options::ThrottlingRetryOptions> {
    let count = non_negative(
        "maxRetryAttemptsOnThrottledRequests",
        config.max_retry_attempts_on_throttled_requests,
    )?
    .map(u32::try_from)
    .transpose()?
    .unwrap_or(JAVA_MAX_RETRY_ATTEMPTS_ON_THROTTLED_REQUESTS);
    let wait = non_negative("maxRetryWaitTimeInSeconds", config.max_retry_wait_time_in_seconds)?
        .map(Duration::from_secs)
        .unwrap_or(JAVA_MAX_RETRY_WAIT_TIME);
    Ok(ThrottlingRetryOptionsBuilder::new()
        .with_max_retry_count(count)
        .with_max_retry_wait_time(wait)
        .build())
}

fn is_emulator_host(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

/// The text of a field value. Strings are returned without JSON quoting.
fn field_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn to_record(document: Map<String, Value>, fields: Option<&HashSet<String>>) -> Record {
    document
        .into_iter()
        .filter(|(k, _)| fields.is_none_or(|f| f.contains(k)))
        .map(|(k, v)| {
            let text = field_text(&v);
            (k, text)
        })
        .collect()
}

/// `createSelectTop`: `SELECT TOP n * ` or `SELECT TOP n r['f1'] , r['f2'] `.
pub fn select_top(fields: Option<&HashSet<String>>, top: i32) -> String {
    match fields {
        None => format!("SELECT TOP {top} * "),
        Some(fields) => {
            let mut sql = format!("SELECT TOP {top} ");
            let initial_len = sql.len();
            for field in fields {
                if sql.len() != initial_len {
                    sql.push_str(", ");
                }
                sql.push_str("r['");
                sql.push_str(field);
                sql.push_str("'] ");
            }
            sql
        }
    }
}

impl Db for CosmosDb {
    async fn init(&self) -> Result<()> {
        let config = self.config().clone();
        let state = self
            .shared
            .connection
            .get_or_init(|| async move { connect(&config).await.map_err(|e| format!("{e:#}")) })
            .await;
        state.as_ref().map(|_| ()).map_err(|e| anyhow!("{e}"))
    }

    async fn cleanup(&self) -> Result<()> {
        Ok(())
    }

    async fn read(&self, table: &str, key: &str, fields: Option<&HashSet<String>>, result: &mut Record) -> Status {
        let outcome = async {
            let container = self.container(table).await?;
            self.read_document(&container, key).await
        }
        .await;
        match outcome {
            Ok((document, _)) => {
                result.extend(to_record(document, fields));
                Status::Ok
            }
            Err(e) => {
                error!(
                    "Failed to read key {key} in collection {table} in database {} {e}",
                    self.config().database_name
                );
                Status::NotFound
            }
        }
    }

    async fn scan(
        &self,
        table: &str,
        start_key: &str,
        record_count: i32,
        fields: Option<&HashSet<String>>,
        result: &mut Vec<Record>,
    ) -> Status {
        let outcome = async {
            let container = self.container(table).await?;
            let sql = format!(
                "{} FROM root r WHERE r.id >= @startkey",
                select_top(fields, record_count)
            );
            let query = Query::from(sql)
                .with_parameter("@startkey", start_key)
                .map_err(|e| self.describe_error(&e))?;
            // The SDK refuses new cross-partition queries spanning more than 100 physical
            // partitions unless told otherwise; YCSB scans always span the whole container.
            let mut feed = FeedOptions::default().with_max_fan_out(u32::MAX);
            if let Some(page_size) = u32::try_from(self.config().preferred_page_size)
                .ok()
                .and_then(NonZeroU32::new)
            {
                feed = feed.with_max_item_count(MaxItemCountHint::Limit(page_size));
            }
            let options = QueryOptions::default().with_feed_options(feed);
            let started = Instant::now();
            let mut pages = container
                .query_items::<Map<String, Value>>(query, FeedScope::full_container(), Some(options))
                .await
                .map_err(|e| self.describe_error(&e))?
                .into_pages();
            while let Some(page) = pages.next().await {
                let page = page.map_err(|e| self.describe_error(&e))?;
                self.after_response("QUERY", started, &page.diagnostics());
                for document in page.into_items() {
                    result.push(to_record(document, None));
                }
            }
            Ok::<(), String>(())
        }
        .await;
        match outcome {
            Ok(()) => Status::Ok,
            Err(e) => {
                error!(
                    "Failed to query key {start_key} from collection {table} in database {} {e}",
                    self.config().database_name
                );
                Status::Error
            }
        }
    }

    async fn update(&self, table: &str, key: &str, values: &Values) -> Status {
        match self.config().update_mode {
            UpdateMode::Replace => self.update_with_replace(table, key, values).await,
            UpdateMode::Patch => self.update_with_patch(table, key, values).await,
        }
    }

    async fn insert(&self, table: &str, key: &str, values: &Values) -> Status {
        let outcome = async {
            let container = self.container(table).await?;
            let mut document = Map::with_capacity(values.len() + 1);
            document.insert("id".to_string(), Value::String(key.to_string()));
            for (field, value) in values {
                document.insert(field.clone(), Value::String(value.clone()));
            }
            let started = Instant::now();
            let response = if self.config().use_upsert {
                container.upsert_item(key.to_string(), key, document, None).await
            } else {
                container.create_item(key.to_string(), key, document, None).await
            }
            .map_err(|e| self.describe_error(&e))?;
            self.after_response("CREATE", started, &response.diagnostics());
            Ok::<(), String>(())
        }
        .await;
        match outcome {
            Ok(()) => Status::Ok,
            Err(e) => {
                error!(
                    "Failed to insert key {key} to collection {table} in database {} {e}",
                    self.config().database_name
                );
                Status::Error
            }
        }
    }

    async fn delete(&self, table: &str, key: &str) -> Status {
        let outcome = async {
            let container = self.container(table).await?;
            let started = Instant::now();
            let response = container
                .delete_item(key.to_string(), key, None)
                .await
                .map_err(|e| self.describe_error(&e))?;
            self.after_response("DELETE", started, &response.diagnostics());
            Ok::<(), String>(())
        }
        .await;
        match outcome {
            Ok(()) => Status::Ok,
            Err(e) => {
                error!(
                    "Failed to delete key {key} in collection {table} database {} {e}",
                    self.config().database_name
                );
                Status::Error
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(text: &str) -> Properties {
        let mut p = Properties::new();
        p.load_str(text);
        p
    }

    const REQUIRED: &str = "azurecosmos.uri = https://acct.documents.azure.com:443/\nazurecosmos.primaryKey = a2V5\n";

    #[test]
    fn from_properties_applies_java_defaults() {
        let c = CosmosConfig::from_properties(&props(REQUIRED)).unwrap();
        assert_eq!(c.database_name, "ycsb");
        assert!(!c.use_upsert);
        assert!(!c.use_gateway);
        assert_eq!(c.consistency_level, ConsistencyLevel::Session);
        assert_eq!(c.user_agent, "azurecosmos-ycsb");
        assert_eq!(c.max_degree_of_parallelism, -1);
        assert_eq!(c.max_buffered_item_count, 0);
        assert_eq!(c.preferred_page_size, -1);
        assert_eq!(c.update_mode, UpdateMode::Replace);
        assert!(c.preferred_regions.is_empty());
        assert_eq!(c.binary_encoding, None);
        assert!(!c.session_capturing_disabled);
    }

    #[test]
    fn from_properties_requires_key_and_uri() {
        let missing_key = CosmosConfig::from_properties(&props("azurecosmos.uri = https://a/\n")).unwrap_err();
        assert!(missing_key.to_string().contains("Missing primary key"));
        let missing_uri = CosmosConfig::from_properties(&props("azurecosmos.primaryKey = k\n")).unwrap_err();
        assert!(missing_uri.to_string().contains("Missing uri"));
    }

    #[test]
    fn from_properties_reads_harness_overrides() {
        let c = CosmosConfig::from_properties(&props(&format!(
            "{REQUIRED}azurecosmos.useUpsert = true\nazurecosmos.useGateway = true\n\
             azurecosmos.consistencyLevel = EVENTUAL\nazurecosmos.preferredRegionList = East US, West US 2\n\
             azurecosmos.maxRetryAttemptsOnThrottledRequests = 3\nazurecosmos.updateMode = PATCH\n\
             azurecosmos.diagnosticsLatencyThresholdInMS = 25\nazurecosmos.gatewayMaxConnectionPoolSize = oops\n\
             azurecosmos.sessionCapturingDisabled = true\n"
        )))
        .unwrap();
        assert!(c.use_upsert);
        assert!(c.use_gateway);
        assert_eq!(c.consistency_level, ConsistencyLevel::Eventual);
        assert_eq!(c.preferred_regions, vec!["East US", "West US 2"]);
        assert_eq!(c.max_retry_attempts_on_throttled_requests, 3);
        assert_eq!(c.update_mode, UpdateMode::Patch);
        assert_eq!(c.diagnostics_latency_threshold_in_ms, 25);
        assert!(c.session_capturing_disabled);
        // Unparsable integers fall back to the default, as in the Java binding.
        assert_eq!(c.gateway_max_connection_pool_size, -1);
    }

    #[test]
    fn from_properties_rejects_unknown_consistency_level() {
        assert!(
            CosmosConfig::from_properties(&props(&format!("{REQUIRED}azurecosmos.consistencyLevel = session\n")))
                .is_err()
        );
    }

    #[test]
    fn consistency_levels_map_to_read_strategies() {
        assert_eq!(
            ConsistencyLevel::Session.read_strategy(),
            ReadConsistencyStrategy::Session
        );
        assert_eq!(
            ConsistencyLevel::Strong.read_strategy(),
            ReadConsistencyStrategy::GlobalStrong
        );
        assert_eq!(
            ConsistencyLevel::BoundedStaleness.read_strategy(),
            ReadConsistencyStrategy::LatestCommitted
        );
        assert_eq!(
            ConsistencyLevel::ConsistentPrefix.read_strategy(),
            ReadConsistencyStrategy::Eventual
        );
        assert_eq!(
            ConsistencyLevel::Eventual.read_strategy(),
            ReadConsistencyStrategy::Eventual
        );
    }

    #[test]
    fn select_top_matches_java_query_text() {
        assert_eq!(select_top(None, 10), "SELECT TOP 10 * ");
        let one = HashSet::from(["field3".to_string()]);
        assert_eq!(select_top(Some(&one), 5), "SELECT TOP 5 r['field3'] ");
        let two = HashSet::from(["field1".to_string(), "field2".to_string()]);
        let sql = select_top(Some(&two), 7);
        assert!(
            sql == "SELECT TOP 7 r['field1'] , r['field2'] " || sql == "SELECT TOP 7 r['field2'] , r['field1'] ",
            "{sql}"
        );
    }

    #[test]
    fn to_record_filters_fields_and_unquotes_strings() {
        let document: Map<String, Value> =
            serde_json::from_str(r#"{"id":"user1","field0":"a\"b","field1":"x","_ts":17}"#).unwrap();
        let all = to_record(document.clone(), None);
        assert_eq!(all["field0"], "a\"b");
        assert_eq!(all["_ts"], "17");
        let only = to_record(document, Some(&HashSet::from(["field1".to_string()])));
        assert_eq!(only.len(), 1);
        assert_eq!(only["field1"], "x");
    }

    #[test]
    fn emulator_hosts_are_detected() {
        for (url, expected) in [
            ("http://localhost:8081/", true),
            ("https://127.0.0.1:8081/", true),
            ("http://[::1]:8081/", true),
            ("https://acct.documents.azure.com:443/", false),
        ] {
            assert_eq!(is_emulator_host(&url::Url::parse(url).unwrap()), expected, "{url}");
        }
    }

    #[test]
    fn negative_option_values_other_than_minus_one_are_rejected() {
        assert_eq!(non_negative("x", -1).unwrap(), None);
        assert_eq!(non_negative("x", 5).unwrap(), Some(5));
        assert!(non_negative("x", -2).is_err());
    }

    #[test]
    fn throttling_options_default_to_java_sdk_values() {
        let mut c = CosmosConfig::from_properties(&props(REQUIRED)).unwrap();
        let defaults = throttling_retry_options(&c).unwrap();
        assert_eq!(defaults.max_retry_count, Some(9));
        assert_eq!(defaults.max_retry_wait_time, Some(Duration::from_secs(30)));
        c.max_retry_attempts_on_throttled_requests = 0;
        c.max_retry_wait_time_in_seconds = 5;
        let custom = throttling_retry_options(&c).unwrap();
        assert_eq!(custom.max_retry_count, Some(0));
        assert_eq!(custom.max_retry_wait_time, Some(Duration::from_secs(5)));
        c.max_retry_attempts_on_throttled_requests = -3;
        assert!(throttling_retry_options(&c).is_err());
    }

    #[tokio::test]
    async fn operations_before_init_fail_cleanly() {
        let db = CosmosDb::new(&props(REQUIRED)).unwrap();
        assert_eq!(
            db.read("usertable", "k", None, &mut Record::new()).await,
            Status::NotFound
        );
        assert_eq!(db.insert("usertable", "k", &Values::new()).await, Status::Error);
    }
}
