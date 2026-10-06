//! Port of `site.ycsb.BasicDB`: a binding that talks to no database.
//!
//! Useful for dry runs of a workload file and for exercising the client end to end without
//! a Cosmos DB account. `basicdb.errorrate` (Rust only) makes a fraction of operations fail
//! so failure reporting can be exercised too.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use crate::db::{Db, Record, Values};
use crate::props::Properties;
use crate::rng;
use crate::status::Status;

#[derive(Clone)]
pub struct BasicDb {
    config: Arc<BasicConfig>,
}

struct BasicConfig {
    verbose: bool,
    delay_ms: i32,
    randomize_delay: bool,
    error_rate: f64,
}

impl BasicDb {
    pub fn new(props: &Properties) -> Result<Self> {
        let config = BasicConfig {
            verbose: props.parse_bool("basicdb.verbose", true),
            delay_ms: props.parse_i32("basicdb.simulatedelay", "0")?,
            randomize_delay: props.parse_bool("basicdb.randomizedelay", true),
            error_rate: props.parse_f64("basicdb.errorrate", "0")?,
        };
        if config.verbose {
            println!("***************** properties *****************");
            let mut keys: Vec<&str> = props.keys().collect();
            keys.sort_unstable();
            for k in keys {
                println!("\"{k}\"=\"{}\"", props.get(k).unwrap_or_default());
            }
            println!("**********************************************");
        }
        Ok(Self {
            config: Arc::new(config),
        })
    }

    async fn delay(&self) {
        let delay_ms = self.config.delay_ms;
        if delay_ms <= 0 {
            return;
        }
        let ms = if self.config.randomize_delay {
            rng::next_below(delay_ms as u64)
        } else {
            delay_ms as u64
        };
        if ms > 0 {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    }

    fn outcome(&self) -> Status {
        if self.config.error_rate > 0.0 && rng::next_f64() < self.config.error_rate {
            Status::Error
        } else {
            Status::Ok
        }
    }
}

fn describe_fields(fields: Option<&HashSet<String>>) -> String {
    match fields {
        Some(fields) => fields.iter().map(|f| format!("{f} ")).collect(),
        None => "<all fields>".to_string(),
    }
}

fn describe_values(values: &Values) -> String {
    values.iter().map(|(f, v)| format!("{f}={v} ")).collect()
}

impl Db for BasicDb {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn cleanup(&self) -> Result<()> {
        Ok(())
    }

    async fn read(&self, table: &str, key: &str, fields: Option<&HashSet<String>>, _result: &mut Record) -> Status {
        self.delay().await;
        if self.config.verbose {
            println!("READ {table} {key} [ {}]", describe_fields(fields));
        }
        self.outcome()
    }

    async fn scan(
        &self,
        table: &str,
        start_key: &str,
        record_count: i32,
        fields: Option<&HashSet<String>>,
        _result: &mut Vec<Record>,
    ) -> Status {
        self.delay().await;
        if self.config.verbose {
            println!("SCAN {table} {start_key} {record_count} [ {}]", describe_fields(fields));
        }
        self.outcome()
    }

    async fn update(&self, table: &str, key: &str, values: &Values) -> Status {
        self.delay().await;
        if self.config.verbose {
            println!("UPDATE {table} {key} [ {}]", describe_values(values));
        }
        self.outcome()
    }

    async fn insert(&self, table: &str, key: &str, values: &Values) -> Status {
        self.delay().await;
        if self.config.verbose {
            println!("INSERT {table} {key} [ {}]", describe_values(values));
        }
        self.outcome()
    }

    async fn delete(&self, table: &str, key: &str) -> Status {
        self.delay().await;
        if self.config.verbose {
            println!("DELETE {table} {key}");
        }
        self.outcome()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn basic(props: &str) -> BasicDb {
        let mut p = Properties::new();
        p.load_str(props);
        BasicDb::new(&p).unwrap()
    }

    #[tokio::test]
    async fn operations_succeed_by_default() {
        let db = basic("basicdb.verbose=false");
        assert_eq!(db.read("t", "k", None, &mut Record::new()).await, Status::Ok);
        assert_eq!(db.insert("t", "k", &vec![("f".into(), "v".into())]).await, Status::Ok);
    }

    #[tokio::test]
    async fn error_rate_one_fails_everything() {
        let db = basic("basicdb.verbose=false\nbasicdb.errorrate=1");
        assert_eq!(db.update("t", "k", &Values::new()).await, Status::Error);
        assert_eq!(db.delete("t", "k").await, Status::Error);
    }

    #[tokio::test(start_paused = true)]
    async fn simulated_delay_sleeps() {
        let db = basic("basicdb.verbose=false\nbasicdb.simulatedelay=50\nbasicdb.randomizedelay=false");
        let start = tokio::time::Instant::now();
        db.read("t", "k", None, &mut Record::new()).await;
        assert!(start.elapsed() >= Duration::from_millis(50));
    }
}
