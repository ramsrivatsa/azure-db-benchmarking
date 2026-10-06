//! The database binding interface (`site.ycsb.DB`) and the measuring wrapper (`DBWrapper`).

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use crate::measurements::{IntervalMode, Measurements};
use crate::props::Properties;
use crate::status::Status;

/// One record: field name to value.
pub type Record = HashMap<String, String>;
/// Field values to write, as produced by the workload.
pub type Values = Vec<(String, String)>;

/// A YCSB database binding. One value is created per client task; implementations share
/// expensive state (connections, clients) internally, as the Java bindings do.
pub trait Db: Send + Sync + 'static {
    /// Called once per client task before it issues operations.
    fn init(&self) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Called once per client task after it finished issuing operations.
    fn cleanup(&self) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Reads one record. `fields == None` reads every field.
    fn read(
        &self,
        table: &str,
        key: &str,
        fields: Option<&HashSet<String>>,
        result: &mut Record,
    ) -> impl Future<Output = Status> + Send;

    /// Reads up to `record_count` records starting at `start_key`.
    fn scan(
        &self,
        table: &str,
        start_key: &str,
        record_count: i32,
        fields: Option<&HashSet<String>>,
        result: &mut Vec<Record>,
    ) -> impl Future<Output = Status> + Send;

    /// Overwrites the given fields of an existing record.
    fn update(&self, table: &str, key: &str, values: &Values) -> impl Future<Output = Status> + Send;

    /// Inserts a new record.
    fn insert(&self, table: &str, key: &str, values: &Values) -> impl Future<Output = Status> + Send;

    fn delete(&self, table: &str, key: &str) -> impl Future<Output = Status> + Send;
}

static CLOCK_BASE: OnceLock<Instant> = OnceLock::new();

/// Monotonic nanoseconds since process start, never 0 (0 means "unset" for intended times).
pub fn monotonic_nanos() -> u64 {
    CLOCK_BASE.get_or_init(Instant::now).elapsed().as_nanos() as u64 + 1
}

const REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY: &str = "reportlatencyforeacherror";
const LATENCY_TRACKED_ERRORS_PROPERTY: &str = "latencytrackederrors";
static LOGGED_REPORT_CONFIG: AtomicBool = AtomicBool::new(false);

/// Times every operation and records it under the operation name, or `<OP>-FAILED` when
/// the binding returns a non-OK status, exactly like YCSB's `DBWrapper`.
pub struct DbWrapper<D: Db> {
    db: D,
    measurements: Arc<Measurements>,
    report_latency_for_each_error: bool,
    latency_tracked_errors: HashSet<String>,
    /// Intended start of the current operation under throttling; 0 when not throttled.
    intended_start_ns: AtomicU64,
}

impl<D: Db> DbWrapper<D> {
    pub fn new(db: D, measurements: Arc<Measurements>, props: &Properties) -> Self {
        let report_latency_for_each_error = props.parse_bool(REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY, false);
        let latency_tracked_errors = if report_latency_for_each_error {
            HashSet::new()
        } else {
            props
                .get(LATENCY_TRACKED_ERRORS_PROPERTY)
                .map(|v| v.split(',').map(str::to_string).collect())
                .unwrap_or_default()
        };
        Self {
            db,
            measurements,
            report_latency_for_each_error,
            latency_tracked_errors,
            intended_start_ns: AtomicU64::new(0),
        }
    }

    pub fn measurements(&self) -> &Measurements {
        &self.measurements
    }

    pub async fn init(&self) -> anyhow::Result<()> {
        self.db.init().await?;
        if !LOGGED_REPORT_CONFIG.swap(true, Ordering::Relaxed) {
            let mut tracked: Vec<&str> = self.latency_tracked_errors.iter().map(String::as_str).collect();
            tracked.sort_unstable();
            eprintln!(
                "DBWrapper: report latency for each error is {} and specific error codes to track for latency are: [{}]",
                self.report_latency_for_each_error,
                tracked.join(", ")
            );
        }
        Ok(())
    }

    pub async fn cleanup(&self) -> anyhow::Result<()> {
        let ist = self.intended_start_ns();
        let st = monotonic_nanos();
        let res = self.db.cleanup().await;
        let en = monotonic_nanos();
        self.measure("CLEANUP", Status::Ok, ist, st, en);
        res
    }

    pub async fn read(&self, table: &str, key: &str, fields: Option<&HashSet<String>>, result: &mut Record) -> Status {
        let ist = self.intended_start_ns();
        let st = monotonic_nanos();
        let res = self.db.read(table, key, fields, result).await;
        let en = monotonic_nanos();
        self.measure("READ", res, ist, st, en);
        self.measurements.report_status("READ", res);
        res
    }

    pub async fn scan(
        &self,
        table: &str,
        start_key: &str,
        record_count: i32,
        fields: Option<&HashSet<String>>,
        result: &mut Vec<Record>,
    ) -> Status {
        let ist = self.intended_start_ns();
        let st = monotonic_nanos();
        let res = self.db.scan(table, start_key, record_count, fields, result).await;
        let en = monotonic_nanos();
        self.measure("SCAN", res, ist, st, en);
        self.measurements.report_status("SCAN", res);
        res
    }

    pub async fn update(&self, table: &str, key: &str, values: &Values) -> Status {
        let ist = self.intended_start_ns();
        let st = monotonic_nanos();
        let res = self.db.update(table, key, values).await;
        let en = monotonic_nanos();
        self.measure("UPDATE", res, ist, st, en);
        self.measurements.report_status("UPDATE", res);
        res
    }

    pub async fn insert(&self, table: &str, key: &str, values: &Values) -> Status {
        let ist = self.intended_start_ns();
        let st = monotonic_nanos();
        let res = self.db.insert(table, key, values).await;
        let en = monotonic_nanos();
        self.measure("INSERT", res, ist, st, en);
        self.measurements.report_status("INSERT", res);
        res
    }

    pub async fn delete(&self, table: &str, key: &str) -> Status {
        let ist = self.intended_start_ns();
        let st = monotonic_nanos();
        let res = self.db.delete(table, key).await;
        let en = monotonic_nanos();
        self.measure("DELETE", res, ist, st, en);
        self.measurements.report_status("DELETE", res);
        res
    }

    /// `Measurements.getIntendedStartTimeNs`: 0 in `op` mode, otherwise the throttled
    /// schedule's start time, or now when not throttled.
    pub fn intended_start_ns(&self) -> u64 {
        if self.measurements.interval_mode() == IntervalMode::Op {
            return 0;
        }
        match self.intended_start_ns.load(Ordering::Relaxed) {
            0 => monotonic_nanos(),
            t => t,
        }
    }

    pub fn set_intended_start_ns(&self, t: u64) {
        if self.measurements.interval_mode() == IntervalMode::Op {
            return;
        }
        self.intended_start_ns.store(t, Ordering::Relaxed);
    }

    fn measure(&self, op: &str, result: Status, ist: u64, st: u64, en: u64) {
        let name: Cow<'_, str> = if result.is_ok() {
            Cow::Borrowed(op)
        } else if self.report_latency_for_each_error || self.latency_tracked_errors.contains(result.name()) {
            Cow::Owned(format!("{op}-{}", result.name()))
        } else {
            Cow::Owned(format!("{op}-FAILED"))
        };
        self.measurements.measure(&name, en.saturating_sub(st) / 1000);
        self.measurements.measure_intended(&name, en.saturating_sub(ist) / 1000);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(Status);

    impl Db for Fixed {
        async fn init(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn cleanup(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn read(&self, _: &str, _: &str, _: Option<&HashSet<String>>, _: &mut Record) -> Status {
            self.0
        }
        async fn scan(&self, _: &str, _: &str, _: i32, _: Option<&HashSet<String>>, _: &mut Vec<Record>) -> Status {
            self.0
        }
        async fn update(&self, _: &str, _: &str, _: &Values) -> Status {
            self.0
        }
        async fn insert(&self, _: &str, _: &str, _: &Values) -> Status {
            self.0
        }
        async fn delete(&self, _: &str, _: &str) -> Status {
            self.0
        }
    }

    fn export_text(m: &Measurements) -> String {
        let mut out = Vec::new();
        m.export(&mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    fn wrapper(status: Status, props: &str) -> DbWrapper<Fixed> {
        let mut p = Properties::new();
        p.load_str(props);
        let m = Arc::new(Measurements::from_properties(&p).unwrap());
        DbWrapper::new(Fixed(status), m, &p)
    }

    #[tokio::test]
    async fn failed_operations_are_measured_under_failed_name() {
        let w = wrapper(Status::NotFound, "");
        assert_eq!(w.read("t", "k", None, &mut Record::new()).await, Status::NotFound);
        let text = export_text(w.measurements());
        assert!(text.contains("[READ-FAILED], Operations, 1\n"), "{text}");
        assert!(text.contains("[READ], Return=NOT_FOUND, 1\n"), "{text}");
        // Like Java, the status report creates an empty READ measurement.
        assert!(text.contains("[READ], Operations, 0\n"), "{text}");
    }

    #[tokio::test]
    async fn tracked_errors_get_their_own_measurement() {
        let w = wrapper(Status::NotFound, "latencytrackederrors=NOT_FOUND");
        w.read("t", "k", None, &mut Record::new()).await;
        assert!(export_text(w.measurements()).contains("[READ-NOT_FOUND], Operations, 1\n"));
    }

    #[tokio::test]
    async fn successful_operations_and_cleanup_are_measured() {
        let w = wrapper(Status::Ok, "");
        w.insert("t", "k", &Values::new()).await;
        w.update("t", "k", &Values::new()).await;
        w.scan("t", "k", 5, None, &mut Vec::new()).await;
        w.delete("t", "k").await;
        w.cleanup().await.unwrap();
        let text = export_text(w.measurements());
        for op in ["INSERT", "UPDATE", "SCAN", "DELETE", "CLEANUP"] {
            assert!(text.contains(&format!("[{op}], Operations, 1\n")), "{op}: {text}");
        }
    }

    #[tokio::test]
    async fn intended_start_is_ignored_in_op_mode() {
        let w = wrapper(Status::Ok, "");
        w.set_intended_start_ns(42);
        assert_eq!(w.intended_start_ns(), 0);
        let w = wrapper(Status::Ok, "measurement.interval=both");
        w.set_intended_start_ns(42);
        assert_eq!(w.intended_start_ns(), 42);
        w.set_intended_start_ns(0);
        assert!(w.intended_start_ns() > 0);
    }
}
