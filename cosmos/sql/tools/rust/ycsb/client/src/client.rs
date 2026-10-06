//! The benchmark driver: `site.ycsb.Client`, `ClientThread` and `StatusThread`.
//!
//! Each YCSB "thread" is a tokio task that issues one operation at a time, so `-threads N`
//! keeps N operations in flight exactly as the Java client does. Throttling (`-target`),
//! the periodic status line (`-s`), `maxexecutiontime` and the final measurement export
//! follow the Java implementation.

use std::fs::File;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::db::{monotonic_nanos, Db, DbWrapper};
use crate::measurements::{Measurements, MEASUREMENT_TYPE_PROPERTY};
use crate::props::Properties;
use crate::utils::{decimal_format_2, java_double_to_string};
use crate::workload::{CoreWorkload, INSERT_COUNT_PROPERTY, OPERATION_COUNT_PROPERTY, RECORD_COUNT_PROPERTY};

pub const THREAD_COUNT_PROPERTY: &str = "threadcount";
pub const TARGET_PROPERTY: &str = "target";
pub const DO_TRANSACTIONS_PROPERTY: &str = "dotransactions";
pub const STATUS_PROPERTY: &str = "status";
pub const LABEL_PROPERTY: &str = "label";
pub const MAX_EXECUTION_TIME_PROPERTY: &str = "maxexecutiontime";
pub const EXPORT_FILE_PROPERTY: &str = "exportfile";
pub const EXPORTER_PROPERTY: &str = "exporter";
const TEXT_EXPORTER: &str = "site.ycsb.measurements.exporter.TextMeasurementsExporter";

/// Outcome of a load or run phase.
#[derive(Debug)]
pub struct RunOutcome {
    pub operations: u64,
    pub runtime_ms: u64,
    /// Tasks whose binding failed to initialize (they performed no operations).
    pub failed_inits: usize,
}

/// Per-task progress, read by the status thread.
struct Progress {
    ops_done: Vec<AtomicU64>,
    op_counts: Vec<u64>,
}

impl Progress {
    fn totals(&self) -> (u64, u64) {
        let mut done = 0;
        let mut todo = 0;
        for (counter, &count) in self.ops_done.iter().zip(&self.op_counts) {
            let d = counter.load(Ordering::Relaxed);
            done += d;
            todo += count.saturating_sub(d);
        }
        (done, todo)
    }
}

/// `CountDownLatch` for the status thread to wait on client completion.
struct Latch {
    remaining: Mutex<usize>,
    done: Condvar,
}

impl Latch {
    fn new(count: usize) -> Self {
        Self {
            remaining: Mutex::new(count),
            done: Condvar::new(),
        }
    }

    fn count_down(&self) {
        let mut remaining = self.remaining.lock().expect("latch lock poisoned");
        *remaining = remaining.saturating_sub(1);
        if *remaining == 0 {
            self.done.notify_all();
        }
    }

    /// Waits until the count reaches zero or the deadline passes. Returns true if done.
    fn wait_until(&self, deadline: Instant) -> bool {
        let mut remaining = self.remaining.lock().expect("latch lock poisoned");
        while *remaining > 0 {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            remaining = self
                .done
                .wait_timeout(remaining, deadline - now)
                .expect("latch lock poisoned")
                .0;
        }
        true
    }
}

/// Runs the load (`dotransactions=false`) or transaction phase and exports the results.
pub async fn run<D: Db + Clone>(
    props: Arc<Properties>,
    db: D,
    workload: Arc<CoreWorkload>,
    measurements: Arc<Measurements>,
) -> Result<RunOutcome> {
    let status = props.parse_bool(STATUS_PROPERTY, false);
    let label = props.get_or(LABEL_PROPERTY, "").to_string();
    let max_execution_time = i64::from(props.parse_i32(MAX_EXECUTION_TIME_PROPERTY, "0")?);
    let mut thread_count = props.parse_i32(THREAD_COUNT_PROPERTY, "1")?.max(1);
    let target = props.parse_i32(TARGET_PROPERTY, "0")?;
    let target_per_thread_per_ms = if target > 0 {
        f64::from(target) / f64::from(thread_count) / 1000.0
    } else {
        -1.0
    };
    let do_transactions = props.parse_bool(DO_TRANSACTIONS_PROPERTY, true);

    let op_count: i64 = if do_transactions {
        props.parse_i64(OPERATION_COUNT_PROPERTY, "0")?
    } else if props.contains(INSERT_COUNT_PROPERTY) {
        props.parse_i64(INSERT_COUNT_PROPERTY, "0")?
    } else {
        props.parse_i64(RECORD_COUNT_PROPERTY, "0")?
    };
    let op_count = op_count.max(0) as u64;
    if u64::from(thread_count.unsigned_abs()) > op_count && op_count > 0 {
        thread_count = op_count as i32;
        println!("Warning: the threadcount is bigger than recordcount, the threadcount will be recordcount!");
    }
    let thread_count = thread_count as usize;
    let per_thread_counts: Vec<u64> = (0..thread_count)
        .map(|id| op_count / thread_count as u64 + u64::from((id as u64) < op_count % thread_count as u64))
        .collect();

    let progress = Arc::new(Progress {
        ops_done: (0..thread_count).map(|_| AtomicU64::new(0)).collect(),
        op_counts: per_thread_counts.clone(),
    });
    let latch = Arc::new(Latch::new(thread_count));

    let status_thread = status.then(|| {
        let standard_status = props.get(MEASUREMENT_TYPE_PROPERTY) == Some("timeseries");
        let interval = Duration::from_secs(u64::from(
            props.parse_i32("status.interval", "10").unwrap_or(10).max(1) as u32
        ));
        let progress = Arc::clone(&progress);
        let latch = Arc::clone(&latch);
        let measurements = Arc::clone(&measurements);
        let label = label.clone();
        std::thread::Builder::new()
            .name("status".into())
            .spawn(move || status_loop(&label, standard_status, interval, &progress, &latch, &measurements))
            .expect("spawning the status thread")
    });

    let started = Instant::now();
    let mut tasks = Vec::with_capacity(thread_count);
    for (id, &count) in per_thread_counts.iter().enumerate() {
        let wrapper = DbWrapper::new(db.clone(), Arc::clone(&measurements), &props);
        let workload = Arc::clone(&workload);
        let progress = Arc::clone(&progress);
        let latch = Arc::clone(&latch);
        tasks.push(tokio::spawn(async move {
            let result = client_task(
                id,
                wrapper,
                workload,
                do_transactions,
                count,
                target_per_thread_per_ms,
                &progress,
            )
            .await;
            latch.count_down();
            result
        }));
    }

    let terminator = (max_execution_time > 0).then(|| {
        let workload = Arc::clone(&workload);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(max_execution_time as u64)).await;
            eprintln!("Maximum time elapsed. Requesting stop for the workload.");
            workload.request_stop();
            eprintln!("Stop requested for workload. Now Joining!");
        })
    });

    let mut failed_inits = 0;
    let mut fatal = None;
    for task in tasks {
        match task.await {
            Ok(Ok(TaskEnd::Completed)) => {}
            Ok(Ok(TaskEnd::InitFailed)) => failed_inits += 1,
            Ok(Err(e)) => fatal = Some(e),
            Err(join_error) => fatal = Some(anyhow::anyhow!("client task panicked: {join_error}")),
        }
    }
    let runtime_ms = started.elapsed().as_millis() as u64;
    if let Some(terminator) = terminator {
        terminator.abort();
    }
    if let Some(handle) = status_thread {
        handle.join().expect("status thread panicked");
    }
    if let Some(e) = fatal {
        return Err(e);
    }

    let (operations, _) = progress.totals();
    export_measurements(&props, &measurements, operations, runtime_ms).context("Could not export measurements")?;
    Ok(RunOutcome {
        operations,
        runtime_ms,
        failed_inits,
    })
}

enum TaskEnd {
    Completed,
    InitFailed,
}

static REPORTED_INIT_ERRORS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// `ClientThread.run`.
async fn client_task<D: Db>(
    id: usize,
    db: DbWrapper<D>,
    workload: Arc<CoreWorkload>,
    do_transactions: bool,
    op_count: u64,
    target_per_ms: f64,
    progress: &Progress,
) -> Result<TaskEnd> {
    if let Err(e) = db.init().await {
        // Every task usually fails the same way; report each distinct error once.
        let message = format!("{e:#}");
        let mut reported = REPORTED_INIT_ERRORS.lock().expect("init error lock poisoned");
        if !reported.contains(&message) {
            eprintln!("Error initializing the database binding for client {id}: {message}");
            reported.push(message);
        }
        return Ok(TaskEnd::InitFailed);
    }

    let tick_ns = (target_per_ms > 0.0).then(|| (1_000_000.0 / target_per_ms) as u64);
    // Spread the clients out so they don't all hit the database at the same instant.
    if let Some(tick) = tick_ns.filter(|_| target_per_ms <= 1.0) {
        let delay = crate::rng::next_below(tick.min(i32::MAX as u64));
        sleep_until(monotonic_nanos() + delay).await;
    }

    let start = monotonic_nanos();
    let mut done = 0u64;
    while (op_count == 0 || done < op_count) && !workload.is_stop_requested() {
        let ok = if do_transactions {
            workload.do_transaction(&db).await?
        } else {
            workload.do_insert(&db).await
        };
        if !ok {
            break;
        }
        done += 1;
        progress.ops_done[id].store(done, Ordering::Relaxed);
        if let Some(tick) = tick_ns {
            let deadline = start + done * tick;
            sleep_until(deadline).await;
            db.set_intended_start_ns(deadline);
        }
    }

    db.set_intended_start_ns(0);
    if let Err(e) = db.cleanup().await {
        eprintln!("Error cleaning up the database binding for client {id}: {e:#}");
    }
    Ok(TaskEnd::Completed)
}

async fn sleep_until(deadline_ns: u64) {
    let now = monotonic_nanos();
    if deadline_ns > now {
        tokio::time::sleep(Duration::from_nanos(deadline_ns - now)).await;
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `StatusThread.run`: one line per interval, plus a final line once every client is done.
fn status_loop(
    label: &str,
    standard_status: bool,
    interval: Duration,
    progress: &Progress,
    latch: &Latch,
    measurements: &Measurements,
) {
    let start_ms = current_time_millis();
    let mut deadline = Instant::now() + interval;
    let mut start_interval_ms = start_ms;
    let mut last_total_ops = 0;
    loop {
        let now_ms = current_time_millis();
        last_total_ops = print_status(
            label,
            standard_status,
            start_ms,
            start_interval_ms,
            now_ms,
            last_total_ops,
            progress,
            measurements,
        );
        let all_done = latch.wait_until(deadline);
        start_interval_ms = now_ms;
        deadline += interval;
        if all_done {
            break;
        }
    }
    print_status(
        label,
        standard_status,
        start_ms,
        start_interval_ms,
        current_time_millis(),
        last_total_ops,
        progress,
        measurements,
    );
}

#[allow(clippy::too_many_arguments)]
fn print_status(
    label: &str,
    standard_status: bool,
    start_ms: u64,
    start_interval_ms: u64,
    end_interval_ms: u64,
    last_total_ops: u64,
    progress: &Progress,
    measurements: &Measurements,
) -> u64 {
    let (total_ops, todo_ops) = progress.totals();
    let line = status_line(
        label,
        &chrono::Local::now().format("%Y-%m-%d %H:%M:%S:%3f").to_string(),
        start_ms,
        start_interval_ms,
        end_interval_ms,
        total_ops,
        last_total_ops,
        todo_ops,
        &measurements.summary(),
    );
    eprintln!("{line}");
    if standard_status {
        println!("{line}");
    }
    total_ops
}

/// The text of one status line (`StatusThread.computeStats`).
#[allow(clippy::too_many_arguments)]
pub fn status_line(
    label: &str,
    timestamp: &str,
    start_ms: u64,
    start_interval_ms: u64,
    end_interval_ms: u64,
    total_ops: u64,
    last_total_ops: u64,
    todo_ops: u64,
    summary: &str,
) -> String {
    let interval = end_interval_ms.saturating_sub(start_ms);
    let throughput = 1000.0 * (total_ops as f64 / interval as f64);
    let current_throughput = 1000.0
        * ((total_ops - last_total_ops.min(total_ops)) as f64
            / end_interval_ms.saturating_sub(start_interval_ms) as f64);
    // `(long) Math.ceil(todo / throughput)`: saturates on infinity and maps NaN to 0, as in Java.
    let est_remaining = (todo_ops as f64 / throughput).ceil() as i64;

    let mut msg = format!("{label}{timestamp} {} sec: {total_ops} operations; ", interval / 1000);
    if total_ops != 0 {
        msg.push_str(&decimal_format_2(current_throughput));
        msg.push_str(" current ops/sec; ");
    }
    if todo_ops != 0 {
        msg.push_str("est completion in ");
        msg.push_str(&format_remaining(est_remaining));
    }
    msg.push_str(summary);
    msg
}

/// `RemainingFormatter.format`, including its quirks (seconds only shown under a minute).
pub fn format_remaining(mut seconds: i64) -> String {
    let mut time = String::new();
    let days = seconds / 86_400;
    if days > 0 {
        time.push_str(&format!("{days}{}", if days == 1 { " day " } else { " days " }));
        seconds -= days * 86_400;
    }
    let hours = seconds / 3_600;
    if hours > 0 {
        time.push_str(&format!("{hours}{}", if hours == 1 { " hour " } else { " hours " }));
        seconds -= hours * 3_600;
    }
    if days < 1 {
        let minutes = seconds / 60;
        if minutes > 0 {
            time.push_str(&format!(
                "{minutes}{}",
                if minutes == 1 { " minute " } else { " minutes " }
            ));
            seconds -= minutes * 60;
        }
    }
    if time.is_empty() {
        // Java appends the number before evaluating `time.length() == 1`, so single-digit
        // values come out singular ("5 second ").
        let digits = seconds.to_string();
        let unit = if digits.len() == 1 { " second " } else { " seconds " };
        time.push_str(&digits);
        time.push_str(unit);
    }
    time
}

/// `Client.exportMeasurements` with the text exporter.
fn export_measurements(
    props: &Properties,
    measurements: &Measurements,
    operations: u64,
    runtime_ms: u64,
) -> io::Result<()> {
    if let Some(exporter) = props.get(EXPORTER_PROPERTY).filter(|e| *e != TEXT_EXPORTER) {
        eprintln!("Could not find exporter {exporter}, will use default text reporter.");
    }
    let mut out: Box<dyn Write> = match props.get(EXPORT_FILE_PROPERTY) {
        Some(path) => Box::new(io::BufWriter::new(File::create(path)?)),
        None => Box::new(io::BufWriter::new(io::stdout().lock())),
    };
    writeln!(out, "[OVERALL], RunTime(ms), {runtime_ms}")?;
    let throughput = 1000.0 * operations as f64 / runtime_ms as f64;
    writeln!(
        out,
        "[OVERALL], Throughput(ops/sec), {}",
        java_double_to_string(throughput)
    )?;
    measurements.export(&mut out)?;
    out.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_line_matches_java_format() {
        let line = status_line(
            "",
            "2022-04-13 16:16:13:684",
            0,
            0,
            10_000,
            15_743,
            0,
            47_000,
            "[READ: Count=14959, Max=613887, Min=1110, Avg=12876.19, 90=25807, 99=78143, 99.9=493311, 99.99=609279] ",
        );
        assert_eq!(
            line,
            "2022-04-13 16:16:13:684 10 sec: 15743 operations; 1574.3 current ops/sec; est completion in 30 seconds \
             [READ: Count=14959, Max=613887, Min=1110, Avg=12876.19, 90=25807, 99=78143, 99.9=493311, 99.99=609279] "
        );
    }

    #[test]
    fn status_line_omits_rates_before_any_operation_and_estimate_when_done() {
        let first = status_line("", "T", 1000, 1000, 1000, 0, 0, 100, "");
        assert_eq!(first, "T 0 sec: 0 operations; est completion in 0 second ");
        let last = status_line("lbl ", "T", 0, 9_000, 10_000, 100, 90, 0, "[X] ");
        assert_eq!(last, "lbl T 10 sec: 100 operations; 10 current ops/sec; [X] ");
    }

    #[test]
    fn format_remaining_matches_remaining_formatter() {
        assert_eq!(format_remaining(5), "5 second ");
        assert_eq!(format_remaining(30), "30 seconds ");
        assert_eq!(format_remaining(61), "1 minute ");
        assert_eq!(format_remaining(3_720), "1 hour 2 minutes ");
        assert_eq!(format_remaining(90_000), "1 day 1 hour ");
        assert_eq!(format_remaining(2 * 86_400 + 59), "2 days ");
    }

    #[test]
    fn latch_reports_completion() {
        let latch = Latch::new(2);
        assert!(!latch.wait_until(Instant::now() + Duration::from_millis(10)));
        latch.count_down();
        latch.count_down();
        assert!(latch.wait_until(Instant::now() + Duration::from_secs(1)));
    }

    #[test]
    fn progress_totals_sum_done_and_remaining() {
        let p = Progress {
            ops_done: vec![AtomicU64::new(3), AtomicU64::new(5)],
            op_counts: vec![10, 5],
        };
        assert_eq!(p.totals(), (8, 7));
        let unbounded = Progress {
            ops_done: vec![AtomicU64::new(3)],
            op_counts: vec![0],
        };
        assert_eq!(unbounded.totals(), (3, 0));
    }
}
