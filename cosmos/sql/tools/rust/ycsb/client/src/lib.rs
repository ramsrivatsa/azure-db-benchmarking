//! A Rust port of the YCSB core workload and client, with the Azure Cosmos DB binding.
//!
//! Derived from YCSB (<https://github.com/brianfrankcooper/YCSB>, Apache License 2.0) and the
//! Azure fork's Cosmos DB binding (<https://github.com/Azure/YCSB>).

pub mod bindings;
pub mod cli;
pub mod client;
pub mod db;
pub mod generator;
pub mod measurements;
pub mod props;
pub mod rng;
pub mod status;
pub mod utils;
pub mod workload;
