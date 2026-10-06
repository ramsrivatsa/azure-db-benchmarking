# YCSB for Azure Cosmos DB in Rust

A Rust port of the [YCSB](https://github.com/brianfrankcooper/YCSB) core workload and client, with the Azure Cosmos DB (NoSQL API) binding rebuilt on the [Azure Cosmos DB Rust SDK](https://crates.io/crates/azure_data_cosmos). It reads the same workload and property files and prints the same output as the Java client, so it plugs into this repository's benchmarking framework and its result processing unchanged.

| Path | Contents |
| --- | --- |
| [`client/`](./client) | The `ycsb` binary (Cargo project) |
| [`client/workloads/`](./client/workloads) | The standard YCSB workloads (`workloada`..`workloadf`) |
| [`client/conf/azurecosmos.properties`](./client/conf/azurecosmos.properties) | Binding properties, with notes on how each maps to the Rust SDK |
| [`recipes/`](./recipes) | ARM recipes that run this client on Azure VMs |

## Running it

Build (Rust 1.88 or newer):

```bash
cd client
cargo build --release
```

The command line matches YCSB's `bin/ycsb.sh`:

```bash
./target/release/ycsb load azurecosmos -P workloads/workloada -P conf/azurecosmos.properties \
    -p azurecosmos.uri=https://<account>.documents.azure.com:443/ -p azurecosmos.primaryKey=<key> -s -threads 8
./target/release/ycsb run azurecosmos -P workloads/workloada -P conf/azurecosmos.properties \
    -p azurecosmos.uri=https://<account>.documents.azure.com:443/ -p azurecosmos.primaryKey=<key> -s -threads 8 -target 1000
```

As with the Java binding, the database (`ycsb` by default) and a container partitioned on `/id` (named after the `table` property, `usertable` by default) must already exist. `cargo run --release --example create_container -- <uri> <key>` creates them.

The `basic` binding issues no requests, which is handy for checking a workload file: `ycsb run basic -P workloads/workloada -p basicdb.verbose=false -s`. `basicdb.simulatedelay` (ms) and the Rust-only `basicdb.errorrate` simulate latency and failures.

`RUST_LOG` controls client and SDK logging on stderr (default `warn,ycsb_cosmos=info`).

### Against the local emulator

```bash
docker run -d -p 8081:8081 mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview
KEY='C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=='
cargo run --release --example create_container -- http://localhost:8081 "$KEY"
./target/release/ycsb load azurecosmos -P workloads/workloada -p azurecosmos.uri=http://localhost:8081 \
    -p azurecosmos.primaryKey="$KEY" -p recordcount=1000 -s -threads 8
```

Binary JSON encoding is switched off automatically for `localhost` endpoints because the Linux emulator does not support it.

## In the benchmarking framework

The shared [infrastructure template](../../../../infra/azuredeploy.json) has a `benchmarkClient` parameter (`java` by default). With `rust`, each VM installs a Rust toolchain if needed, builds `client/` from the benchmarking-tools repository and branch the deployment points at, and runs it through the same [`azurecosmos-run.sh`](../../../../scripts/azurecosmos-run.sh) and result processing as the Java client. The [recipes](./recipes) mirror the Java recipes with `benchmarkClient=rust`.

The first deployment on a VM compiles the client, which adds a few minutes (about two on 2 CPUs in local testing); later runs on the same VM reuse the build.

Each run logs the data-plane transport it used, `Data-plane transport: gateway_v2 (...)` for Gateway 2.0 or `Data-plane transport: gateway (...)` for the standard gateway, with the endpoint and region, so results can be labeled. The recipes keep the Java recipes' thread counts, which were sized for direct-mode latency; if a run falls short of its target rate, raise `threads`.

`updateMode` (`replace` or `patch`) selects how updates are applied (see below). The update recipes default to `patch`, matching the Java recipes.

## Differences from the Java client

The workload, key space, value sizes, operation mix, `-target` rate limiting and output format follow the Java implementation, including details such as the FNV key hash, the `RandomByteIterator` character set, HdrHistogram percentile rounding and the order in which measurements are printed. A dataset loaded by either client can be read by the other. What differs:

- **Connection mode.** The Rust SDK has no direct (TCP) mode. With `azurecosmos.useGateway=false` (the default) requests use Gateway 2.0 (RNTBD over HTTP/2 to a regional proxy) when the account offers it, otherwise the standard gateway; `useGateway=true` always uses the standard gateway. Latencies are therefore not directly comparable with the Java client in direct mode.
- **Consistency.** The SDK sets read consistency per request: `STRONG` → `GlobalStrong`, `BOUNDED_STALENESS` → `LatestCommitted` (region-local quorum read), `SESSION` → `Session`, `CONSISTENT_PREFIX` and `EVENTUAL` → `Eventual`.
- **Updates.** `azurecosmos.updateMode=replace` (the default) reads the item and replaces it if its ETag is unchanged, retrying up to 4 times, like the brianfrankcooper/YCSB binding. `patch` sends one server-side PATCH with a `replace` per field, like the Azure/YCSB binding the Java recipes use (at most 10 fields per update).
- **Throttling.** Rate-limited (429) requests are retried with the Java SDK's defaults (9 retries, 30 seconds in total) unless `azurecosmos.maxRetryAttemptsOnThrottledRequests`/`maxRetryWaitTimeInSeconds` say otherwise; the Rust SDK's own defaults retry much longer.
- **Scans.** The SDK runs the cross-partition `SELECT TOP` query by reading physical partitions one after another, where the Java SDK queries them in parallel, so scan latency on multi-partition containers is not directly comparable.
- **Wire format.** The Rust SDK sends Cosmos binary JSON by default; set `azurecosmos.binaryEncoding=false` for text JSON like the Java SDK.
- **Unsupported options.** `azurecosmos.maxDegreeOfParallelism`, `azurecosmos.maxBufferedItemCount` and `azurecosmos.appInsightConnectionString` have no equivalent and are ignored with a log message. `gatewayMaxConnectionPoolSize` limits idle connections per endpoint, since the Rust HTTP client does not cap concurrent connections. Measurement types other than `hdrhistogram` (the YCSB default) fall back to `hdrhistogram`, and the output has no JVM garbage collection lines.
- **Threads.** Each YCSB thread is an async task issuing one operation at a time, so `-threads N` keeps N operations in flight as in Java, on a multi-threaded runtime sized to the machine's cores.
- **Session tokens limit one process.** With `SESSION` consistency the SDK merges the session token from every response into a container shared by the whole process, under a write lock. In testing on 48-vCPU VMs this capped a client process at about 35k reads/s however many threads it ran. For read-only runs, which don't need read-your-writes, set `azurecosmos.sessionCapturingDisabled=true`: the same VM then reached about 132k reads/s, limited by CPU, and five VMs sustained 687k reads/s against a 700k RU/s container.
- **Read results.** String fields are returned without JSON quotes (the Java binding returns them quoted), which only matters for `dataintegrity=true`.
- **Initialization failures** are reported once and make the process exit with status 1.

## Testing

```bash
cd client
cargo test                                    # unit tests
docker run -d -p 8081:8081 mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview
COSMOS_EMULATOR_ENDPOINT=http://localhost:8081 cargo test --test emulator -- --test-threads=1
```

The emulator tests create and delete their own databases, run every binding operation (including cross-partition scans and server-side PATCH), and drive the `ycsb` binary through a load and a run phase.

[`test/simulate-vm.sh`](./test/simulate-vm.sh) `[rust|java]` exercises the framework end to end without Azure. It runs [`custom-script.sh`](../../../../scripts/custom-script.sh) the way a client VM does, in an Ubuntu 20.04 container limited to 2 CPUs, against a fresh emulator, with the Azure CLI and azcopy stubbed. It prints the resulting `aggregation.csv` and leaves the logs and "uploaded" result files in a local directory. The container clones the current branch, so commit your changes first.
