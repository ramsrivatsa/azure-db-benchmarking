## SQL(Core) API with the Rust YCSB client
These recipes run the same workloads as the [Java recipes](../../../java/ycsb/recipes), but each client VM builds and runs the [Rust YCSB client](../README.md) (a port of YCSB's core workload and Azure Cosmos DB binding on the [Azure Cosmos DB Rust SDK](https://crates.io/crates/azure_data_cosmos)) instead of [YCSB](https://github.com/brianfrankcooper/YCSB). The framework, the parameters, the job status table and the result files are the same, so results can be compared side by side. Each recipe passes `benchmarkClient=rust` to the shared [infrastructure template](../../../../../infra/azuredeploy.json).

 - [read-recipes](./read)
 - [write-recipes](./write)
 - [update-recipes](./update)
 - [scan-recipes](./scan)
 - [build-your-own](./build-your-own)

The Rust SDK has no direct (TCP) connection mode. Clients use Gateway 2.0 when the account offers it and the standard gateway otherwise, so latencies can differ from the Java recipes, which use direct mode. See [differences from the Java client](../README.md#differences-from-the-java-client).

## Try It
A read recipe with a small read workload to familiarize you with the framework. The results should be available in 20-30 minutes after initiating the deployment (the client VM compiles the Rust client first).

1. Create a [Cosmos DB SQL API container](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-portal)

   |  Setting   |  value  |
   | :--:  | :--:  |
   | Database Name | ycsb |
   | Container Name | usertable |
   | Partition Key  | /id |
   | Container Throughput  | Manual |
   | Throughput | 400 RU/s |

2. Create a [storage account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-portal) and note down the connection string
3. Create a [resource group](https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/manage-resource-groups-portal) in the same region as the Cosmos DB account
4. Click the deploy to Azure button and fill in the following missing parameter values:

   |  Parameter   |  Value  |
   | :--:  | :--:  |
   | Resource group | name of the resource group from step 3 |
   | Region | Make sure the region is the same as the Cosmos DB account region |
   | Results Storage Connection String | connection string of the storage account from step 2 |
   | Cosmos URI  | URI of the Cosmos DB account from step 1 |
   | Cosmos Key  | Primary key of the Cosmos DB account from step 1 |
   | Admin Password | Admin account password |

   [![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FAzure%2Fazure-db-benchmarking%2Fmain%2Fcosmos%2Fsql%2Ftools%2Frust%2Fycsb%2Frecipes%2Fread%2Ftry-it-read%2Fazuredeploy.json)

5. Alternatively, you can execute the recipe using [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli).

    ```
    az deployment group create \
       --resource-group "<resource-group-name>" \
       --name "<deployment-name>" \
       --template-uri "https://raw.githubusercontent.com/Azure/azure-db-benchmarking/main/cosmos/sql/tools/rust/ycsb/recipes/read/try-it-read/azuredeploy.json" \
       --parameters \
          adminPassword="<VM-Password>" \
          resultsStorageConnectionString="<Results-Storage-Connection-String>" \
          cosmosURI="<Cosmos-DB-URI>" \
          cosmosKey="<Cosmos-DB-Key>"
    ```

   To run the recipes from a fork or branch, point both the template URI and the `benchmarkingToolsRepoName`/`benchmarkingToolsBranchName` parameters at it; the VMs download every script and build the Rust client from that repository and branch:

    ```
    az deployment group create \
       --resource-group "<resource-group-name>" \
       --name "<deployment-name>" \
       --template-uri "https://raw.githubusercontent.com/<owner>/azure-db-benchmarking/<branch>/cosmos/sql/tools/rust/ycsb/recipes/read/try-it-read/azuredeploy.json" \
       --parameters benchmarkingToolsRepoName="<owner>/azure-db-benchmarking" benchmarkingToolsBranchName="<branch>" \
          adminPassword="<VM-Password>" resultsStorageConnectionString="<Results-Storage-Connection-String>" \
          cosmosURI="<Cosmos-DB-URI>" cosmosKey="<Cosmos-DB-Key>"
    ```

   A [sample parameter file](./parameter-files) can be used instead of inline parameters.

6. Navigate to the storage account created in step 2 to see the job status and results, exactly as described for the [Java recipes](../../../java/ycsb/recipes#try-it). The job status rows use the partition key `ycsb_sql_rust`.

7. Re-executing the recipe with "Skip Load Phase" set to "true", leaving the rest of the parameter values unchanged, runs just the read phase again on the VM from the previous execution. The compiled client is reused, so the rebuild takes seconds.

## Common Errors
1. Following error will appear in "agent.err" in the "/home/benchmarking" of the client VM if an incorrect storage connection string is passed.
   ```
   Error while accessing storage account, exiting from this machine
   ```
2. If the Cosmos DB URI is unreachable or the Cosmos DB key is incorrect (HTTP 401), the client fails to initialize and "agent.out" on the VM (and the YCSB log in the results container) contains
   ```
   Error initializing the database binding for client 0: Could not connect to the Cosmos DB account at https://<account>.documents.azure.com:443/: ...
   ```
   A malformed URI reports `Illegal argument passed in. Check the format of your parameters.` instead.
3. If the `ycsb` database does not exist, the same log contains
   ```
   Error initializing the database binding for client 0: Invalid database name (ycsb) or failed to read database. ...
   ```
4. If the client fails to build, "agent.out" contains the `cargo` error after `########## Building Rust YCSB ##########`.

## Basic Configuration

   |  Parameter   |  Default Value  | Description |
   | :--:  | :--:  | :--: |
   | Project Name | Benchmarking | this will become part of the VM name(ex: Benchmarking-vm1 ) |
   | Location | [resourceGroup().location] | location of the resource group |
   | Results Storage Connection String  |  | connection string of a storage account |
   | Cosmos URI  |  | Cosmos DB account URI |
   | Cosmos Key  |  | Cosmos DB account KEY |
   | VM Size  | varies by recipe | VM size |
   | VM Count | varies by recipe | Number of VMs |
   | Admin Username | benchmarking | The username for the VM's admin account |
   | Admin Password |  | password for the VM's admin account |
   | Threads | varies by recipe | Number of YCSB client threads (concurrent operations per VM) |
   | YCSB Record Count |varies by recipe |Number of records in the dataset at the start of the workload|
   | Target Operations Per Second |varies by recipe | Maximum number of operations per second to be performed by each client/vm |
   | YCSB Operation Count  |varies by recipe |The number of operations to perform in the workload by each client/vm|
   | Benchmarking Tools Repo Name |Azure/azure-db-benchmarking | GitHub repository name for the benchmarking framework and the Rust client |
   | Benchmarking Tools Branch Name | main | GitHub branch name for the benchmarking framework and the Rust client |
   | Skip Load Phase | false | "True" will skip the YCSB load phase. Used to execute the run phase without running load again |
   | Update Mode | replace (patch for update recipes) | `patch`: one server-side PATCH per update (what the Java client does); `replace`: read the item, then replace it if it is unchanged |

## Advanced Configuration
   The default configuration is used to create a VNet and Subnet, but custom configuration can be provided.
   |  Parameter   |  Default Value  | Description |
   | :--:  | :--:  | :--: |
   | Vnet Name | [concat(parameters('projectName'), '-vnet')] | VNet name |
   | Vnet Address Prefixes | 10.2.0.0/16 | VNet address prefix |
   | Vnet Subnet Name | default | subnet name |
   | Vnet Subnet Address Prefix | 10.2.0.0/24 |  subnet address prefix |

## Monitoring
Job status works as for the [Java recipes](../../../java/ycsb/recipes#monitoring): each job is a row in the "<project name>Metadata" table, keyed by the partition key `ycsb_sql_rust` and the job GUID.

## Results
Once the "JobStatus" key has a value of "Finished", the results will be available in a newly created container, with a name of the format "<project name>-<Date>". The files are the same as for the Java recipes:

   |  File   |  Description  |
   | :--:  | :--:  |
   | aggregation.csv | aggregated result from all the clients |
   | Benchmarking-vm<n>-ycsb.log| YCSB log file for the run phase. There will be as many files as the clients|
   | Benchmarking-vm<n>-ycsb.csv | an intermediary CSV file generated from the YCSB log file. Used to produce the final aggregated results |
