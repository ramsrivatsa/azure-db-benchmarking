#!/bin/bash

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Plays the part of a benchmarking VM inside the container started by simulate-vm.sh: installs
# what cloud-init would, then runs custom-script.sh with the environment that
# cosmos/infra/azuredeploy.json passes to it (try-it-read sized by default).

set -u
export DEBIAN_FRONTEND=noninteractive

if [ "$BENCHMARK_CLIENT" = "rust" ]; then
  packages="build-essential pkg-config"
  use_gateway=false
else
  packages="maven"
  # The Linux emulator only serves gateway mode, and its HTTPS certificate is self-signed.
  use_gateway=true
  export JAVA_OPTS="-DCOSMOS.EMULATOR_SERVER_CERTIFICATE_VALIDATION_DISABLED=true"
fi
apt-get update -qq >/dev/null
apt-get install -y -qq $packages curl git python3 python3.10 jq sudo ca-certificates >/dev/null 2>&1
git config --global --add safe.directory '*'
install -m 0755 /vm/stubs/az /vm/stubs/azcopy /usr/local/bin/
mkdir -p /home/benchmarking /stub-logs /results /work
cd /work
cp /git/azure-db-benchmarking/cosmos/scripts/custom-script.sh .

export DB_BINDING_NAME=azurecosmos ADMIN_USER_NAME=benchmarking PROJECT_NAME=benchmarking \
  BENCHMARKING_TOOLS_URL=/git/azure-db-benchmarking BENCHMARKING_TOOLS_BRANCH_NAME="$BRANCH" \
  YCSB_GIT_REPO_URL=https://github.com/Azure/YCSB.git YCSB_GIT_BRANCH_NAME=main GUID=00000000-0000-0000-0000-000000000001 \
  TARGET_OPERATIONS_PER_SECOND="${TARGET:-300}" THREAD_COUNT="${THREADS:-4}" YCSB_OPERATION_COUNT="${OPS:-3000}" \
  WORKLOAD_TYPE="${WORKLOAD:-workloadc}" YCSB_RECORD_COUNT="${RECORDS:-30}" VM_NAME=benchmarking-vm1 VM_COUNT=1 MACHINE_INDEX=1 \
  RESULT_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=stub;AccountKey=c3R1Yg==;EndpointSuffix=core.windows.net" \
  COSMOS_URI="${COSMOS_URI:-http://localhost:8081}" COSMOS_KEY="$COSMOS_KEY" \
  USE_UPSERT=false USE_GATEWAY="$use_gateway" DIAGNOSTICS_LATENCY_THRESHOLD_IN_MS=-1 WRITE_ONLY_OPERATION="${WRITE_ONLY:-false}" \
  READ_PROPORTION="${READP:-}" UPDATE_PROPORTION="${UPDATEP:-}" SCAN_PROPORTION= INSERT_PROPORTION= \
  REQUEST_DISTRIBUTION=uniform INSERT_ORDER=hashed CUSTOM_SCRIPT_URL=unused INCLUDE_EXCEPTION_STACK=false FIELD_COUNT=10 \
  SKIP_LOAD_PHASE=false WAIT_FOR_FAULT_TO_START_IN_SEC=-1 DURATION_OF_FAULT_IN_SEC=-1 DROP_PROBABILITY=0 FAULT_REGION="" \
  DELAY_IN_MS=-1 USER_AGENT=azurecosmos-ycsb CONSISTENCY_LEVEL=SESSION PREFERRED_REGION_LIST="" APP_INSIGHT_CONN_STR="" \
  BENCHMARK_CLIENT UPDATE_MODE="${UPDATE_MODE:-replace}"

start=$(date +%s)
bash custom-script.sh >> /home/benchmarking/agent.out 2>> /home/benchmarking/agent.err
status=$?
echo "custom-script.sh exited with $status after $(( $(date +%s) - start ))s"

rm -rf /out/*
cp -r /home/benchmarking /results /stub-logs /out/
cp /tmp/ycsb.log /out/ 2>/dev/null

aggregation=$(find /results -name aggregation.csv | head -1)
if [ -n "$aggregation" ]; then
  echo "aggregation.csv:"
  cat "$aggregation"
else
  echo "aggregation.csv was not produced; see agent.out and agent.err in the results directory"
  exit 1
fi
