//! Command line handling compatible with YCSB's `bin/ycsb.sh` and `site.ycsb.Client`.
//!
//! `ycsb load azurecosmos -P workloads/workloada -P azurecosmos.properties -s -threads 4`
//! behaves like the Java launcher: the command selects the phase, the binding name selects
//! the database, and the remaining options are parsed exactly like `Client.parseArguments`.

use anyhow::{bail, Context, Result};

use crate::client::{
    DO_TRANSACTIONS_PROPERTY, LABEL_PROPERTY, STATUS_PROPERTY, TARGET_PROPERTY, THREAD_COUNT_PROPERTY,
};
use crate::props::{parse_java_int, Properties};
use crate::workload::{CORE_WORKLOAD_CLASS, WORKLOAD_PROPERTY};

pub const DB_PROPERTY: &str = "db";

pub const USAGE: &str = "\
Usage: ycsb <load|run> <binding> [options]
       ycsb [client options]          (site.ycsb.Client style, e.g. -db azurecosmos -load ...)

Bindings:
  azurecosmos   Azure Cosmos DB for NoSQL (alias: site.ycsb.db.AzureCosmosClient)
  basic         No database; prints or simulates operations (alias: site.ycsb.BasicDB)

Options:
  -threads n: execute using n threads (default: 1) - can also be specified as the
        \"threadcount\" property using -p
  -target n: attempt to do n operations per second (default: unlimited) - can also
       be specified as the \"target\" property using -p
  -load:  run the loading phase of the workload
  -t:  run the transactions phase of the workload (default)
  -db dbname: specify the name of the DB to use (default: basic) - can also be
        specified as the \"db\" property using -p
  -P propertyfile: load properties from the given file. Multiple files can
           be specified, and will be processed in the order specified
  -p name=value:  specify a property to be passed to the DB and workloads;
          multiple properties can be specified, and override any
          values in the propertyfile
  -s:  show status during run (default: no status)
  -l label:  use label for status (e.g. to label one experiment out of a whole batch)

Required properties:
  workload: the name of the workload class to use (site.ycsb.workloads.CoreWorkload)
";

/// What the command line asked for.
#[derive(Debug)]
pub enum Invocation {
    Help,
    Run {
        client_args: Vec<String>,
        props: Properties,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Binding {
    Cosmos,
    Basic,
}

impl Binding {
    pub fn from_properties(props: &Properties) -> Result<Self> {
        match props.get_or(DB_PROPERTY, "basic") {
            "azurecosmos" | "site.ycsb.db.AzureCosmosClient" => Ok(Self::Cosmos),
            "basic" | "site.ycsb.BasicDB" => Ok(Self::Basic),
            other => bail!("Unknown DB {other}"),
        }
    }
}

/// Parses `ycsb` arguments (without the program name).
pub fn parse(args: &[String]) -> Result<Invocation> {
    let Some(first) = args.first() else {
        return Ok(Invocation::Help);
    };
    let client_args: Vec<String> = match first.as_str() {
        "-h" | "--help" | "help" => return Ok(Invocation::Help),
        "load" | "run" => {
            let binding = args
                .get(1)
                .context("Missing binding name, e.g. `ycsb load azurecosmos ...`")?;
            let phase = if first == "load" { "-load" } else { "-t" };
            [phase.to_string(), "-db".to_string(), binding.clone()]
                .into_iter()
                .chain(args[2..].iter().cloned())
                .collect()
        }
        "shell" => bail!("The interactive YCSB shell is not supported by the Rust client."),
        flag if flag.starts_with('-') => args.to_vec(),
        other => bail!(
            "[ERROR] Found unknown command '{other}'\n[ERROR] Expected one of 'load', 'run', or 'shell'. Exiting."
        ),
    };
    let props = parse_client_arguments(&client_args)?;
    Ok(Invocation::Run { client_args, props })
}

/// `Client.parseArguments`: command line properties override properties files.
pub fn parse_client_arguments(args: &[String]) -> Result<Properties> {
    let mut file_props = Properties::new();
    let mut cli_props = Properties::new();
    let mut i = 0;
    let value = |i: usize, flag: &str| -> Result<&String> {
        args.get(i)
            .with_context(|| format!("Missing argument value for {flag}."))
    };
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "-threads" => {
                let raw = value(i + 1, "-threads")?;
                let n = parse_java_int(raw).with_context(|| format!("Invalid -threads value: {raw}"))?;
                cli_props.set(THREAD_COUNT_PROPERTY, n.to_string());
                i += 2;
            }
            "-target" => {
                let raw = value(i + 1, "-target")?;
                let n = parse_java_int(raw).with_context(|| format!("Invalid -target value: {raw}"))?;
                cli_props.set(TARGET_PROPERTY, n.to_string());
                i += 2;
            }
            "-load" => {
                cli_props.set(DO_TRANSACTIONS_PROPERTY, "false");
                i += 1;
            }
            "-t" => {
                cli_props.set(DO_TRANSACTIONS_PROPERTY, "true");
                i += 1;
            }
            "-s" => {
                cli_props.set(STATUS_PROPERTY, "true");
                i += 1;
            }
            "-db" => {
                cli_props.set(DB_PROPERTY, value(i + 1, "-db")?.clone());
                i += 2;
            }
            "-l" => {
                cli_props.set(LABEL_PROPERTY, value(i + 1, "-l")?.clone());
                i += 2;
            }
            "-P" => {
                file_props.load_file(value(i + 1, "-P")?)?;
                i += 2;
            }
            "-p" => {
                let raw = value(i + 1, "-p")?;
                let (name, v) = raw.split_once('=').with_context(|| {
                    "Argument '-p' expected to be in key=value format (e.g., -p operationcount=99999)"
                })?;
                cli_props.set(name, v);
                i += 2;
            }
            other if other.starts_with('-') => bail!("Unknown option {other}"),
            other => bail!(
                "An argument value without corresponding argument specifier (e.g., -p, -s) was found. \
                 We expected an argument specifier and instead found {other}"
            ),
        }
    }
    file_props.merge_from(&cli_props);
    match file_props.get(WORKLOAD_PROPERTY) {
        None => bail!("Missing property: {WORKLOAD_PROPERTY}\nFailed check required properties."),
        Some(CORE_WORKLOAD_CLASS) | Some("core") => {}
        Some(other) => bail!("Unsupported workload class {other}; the Rust client implements {CORE_WORKLOAD_CLASS}"),
    }
    Ok(file_props)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    fn props_file(text: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(text.as_bytes()).unwrap();
        f
    }

    #[test]
    fn parse_translates_ycsb_sh_style_commands() {
        let workload = props_file("workload=site.ycsb.workloads.CoreWorkload\nrecordcount=10\nthreadcount=8\n");
        let path = workload.path().to_str().unwrap();
        let Invocation::Run { client_args, props } = parse(&args(&[
            "load",
            "azurecosmos",
            "-P",
            path,
            "-s",
            "-threads",
            "4",
            "-target",
            "100",
        ]))
        .unwrap() else {
            panic!("expected a run");
        };
        assert_eq!(&client_args[..3], &args(&["-load", "-db", "azurecosmos"])[..]);
        assert_eq!(props.get("dotransactions"), Some("false"));
        assert_eq!(props.get("db"), Some("azurecosmos"));
        assert_eq!(props.get("status"), Some("true"));
        // Command line values override the file.
        assert_eq!(props.get("threadcount"), Some("4"));
        assert_eq!(props.get("target"), Some("100"));
        assert_eq!(props.get("recordcount"), Some("10"));
        assert_eq!(Binding::from_properties(&props).unwrap(), Binding::Cosmos);
    }

    #[test]
    fn parse_run_sets_transactions_and_later_files_win() {
        let a = props_file("workload=site.ycsb.workloads.CoreWorkload\nfieldcount=10\n");
        let b = props_file("fieldcount=3\n");
        let Invocation::Run { props, .. } = parse(&args(&[
            "run",
            "basic",
            "-P",
            a.path().to_str().unwrap(),
            "-P",
            b.path().to_str().unwrap(),
            "-p",
            "operationcount=5",
        ]))
        .unwrap() else {
            panic!("expected a run");
        };
        assert_eq!(props.get("dotransactions"), Some("true"));
        assert_eq!(props.get("fieldcount"), Some("3"));
        assert_eq!(props.get("operationcount"), Some("5"));
        assert_eq!(Binding::from_properties(&props).unwrap(), Binding::Basic);
    }

    #[test]
    fn parse_rejects_bad_input() {
        assert!(matches!(parse(&[]).unwrap(), Invocation::Help));
        assert!(matches!(parse(&args(&["--help"])).unwrap(), Invocation::Help));
        assert!(parse(&args(&["bogus"])).is_err());
        assert!(parse(&args(&["load"])).is_err());
        assert!(parse(&args(&["run", "basic", "-p", "noequals"])).is_err());
        assert!(parse(&args(&["run", "basic", "-threads"])).is_err());
        assert!(parse(&args(&["run", "basic", "-x"])).is_err());
        let err = parse(&args(&["run", "basic", "-p", "recordcount=1"])).unwrap_err();
        assert!(err.to_string().contains("Missing property: workload"));
    }

    #[test]
    fn binding_aliases_and_default() {
        let mut p = Properties::new();
        assert_eq!(Binding::from_properties(&p).unwrap(), Binding::Basic);
        p.set("db", "site.ycsb.db.AzureCosmosClient");
        assert_eq!(Binding::from_properties(&p).unwrap(), Binding::Cosmos);
        p.set("db", "mongodb");
        assert!(Binding::from_properties(&p).is_err());
    }
}
