use std::process::ExitCode;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;
use ycsb_cosmos::bindings::basic::BasicDb;
use ycsb_cosmos::bindings::cosmos::CosmosDb;
use ycsb_cosmos::cli::{self, Binding, Invocation};
use ycsb_cosmos::client;
use ycsb_cosmos::measurements::Measurements;
use ycsb_cosmos::workload::CoreWorkload;

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn,ycsb_cosmos=info")))
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("{e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<ExitCode> {
    let (client_args, props) = match cli::parse(args)? {
        Invocation::Help => {
            print!("{}", cli::USAGE);
            return Ok(ExitCode::SUCCESS);
        }
        Invocation::Run { client_args, props } => (client_args, props),
    };
    eprintln!("Command line: {}", client_args.join(" "));
    eprintln!("YCSB Client {} (Rust)", env!("CARGO_PKG_VERSION"));
    eprintln!();
    eprintln!("Loading workload...");

    let props = Arc::new(props);
    let binding = Binding::from_properties(&props)?;
    let measurements = Arc::new(Measurements::from_properties(&props)?);
    let workload = Arc::new(with_slow_setup_notice(|| {
        CoreWorkload::new(&props, Arc::clone(&measurements))
    })?);
    eprintln!("Starting test.");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting the async runtime")?;
    let outcome = runtime.block_on(async {
        match binding {
            Binding::Cosmos => client::run(Arc::clone(&props), CosmosDb::new(&props)?, workload, measurements).await,
            Binding::Basic => client::run(Arc::clone(&props), BasicDb::new(&props)?, workload, measurements).await,
        }
    })?;
    // Don't wait for idle connection pools and other background work to wind down.
    runtime.shutdown_timeout(Duration::from_secs(1));

    if outcome.failed_inits > 0 {
        eprintln!(
            "{} client(s) failed to initialize the database binding.",
            outcome.failed_inits
        );
        return Ok(ExitCode::FAILURE);
    }
    Ok(ExitCode::SUCCESS)
}

/// Prints YCSB's "might take a few minutes" notice if workload setup runs over two seconds.
fn with_slow_setup_notice<T>(setup: impl FnOnce() -> T) -> T {
    let (done, finished) = mpsc::channel::<()>();
    let notice = std::thread::spawn(move || {
        if finished.recv_timeout(Duration::from_secs(2)) == Err(mpsc::RecvTimeoutError::Timeout) {
            eprintln!(" (might take a few minutes for large data sets)");
        }
    });
    let result = setup();
    drop(done);
    let _ = notice.join();
    result
}
