#!/bin/bash

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Runs cosmos/scripts/custom-script.sh the way a benchmarking VM does, without Azure: an
# Ubuntu 20.04 container (the VM image) runs the script against a fresh local Cosmos DB
# emulator, with the Azure CLI and azcopy replaced by stubs that keep "uploaded" files locally.
#
# Usage: simulate-vm.sh [rust|java]
#
# The container clones the current branch from this repository, so commit changes first.
# Needs Docker and a Rust toolchain on the host. Environment overrides: VM_CPUS (default 2,
# like Standard_D2s_v3), OUT_DIR, EMULATOR_PROTOCOL (http, or https which the Java SDK
# requires; defaults to https for java), and the workload knobs read by vm/run.sh (RECORDS,
# OPS, THREADS, TARGET, WORKLOAD, WRITE_ONLY, READP, UPDATEP, UPDATE_MODE).

set -euo pipefail

client=${1:-rust}
here=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$here" rev-parse --show-toplevel)
branch=$(git -C "$repo" rev-parse --abbrev-ref HEAD)
out=${OUT_DIR:-$(mktemp -d -t ycsb-vm-sim)}
emulator=ycsb-sim-emulator
if [ "$client" = "java" ]; then default_protocol=https; else default_protocol=http; fi
protocol=${EMULATOR_PROTOCOL:-$default_protocol}
endpoint="$protocol://localhost:8081"
key='C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=='

echo "Starting a fresh emulator ($emulator, $protocol)"
docker rm -f "$emulator" >/dev/null 2>&1 || true
docker run -d --name "$emulator" -p 8081:8081 mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview \
  --protocol "$protocol" >/dev/null
until curl -sk -o /dev/null "$endpoint/"; do sleep 2; done

echo "Creating database ycsb and container usertable"
(cd "$here/../client" && cargo run -q --release --example create_container -- "$endpoint" "$key")

echo "Running custom-script.sh for benchmarkClient=$client on branch $branch (results in $out)"
docker run --rm --cpus "${VM_CPUS:-2}" --network "container:$emulator" \
  -v "$repo:/git/azure-db-benchmarking:ro" -v "$here/vm:/vm:ro" -v "$out:/out" \
  -e BENCHMARK_CLIENT="$client" -e BRANCH="$branch" -e COSMOS_URI="$endpoint" -e COSMOS_KEY="$key" \
  -e RECORDS -e OPS -e THREADS -e TARGET -e WORKLOAD -e WRITE_ONLY -e READP -e UPDATEP -e UPDATE_MODE \
  ubuntu:20.04 bash /vm/run.sh

docker rm -f "$emulator" >/dev/null
echo "Results: $out"
