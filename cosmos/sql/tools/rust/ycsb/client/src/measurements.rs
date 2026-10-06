//! Latency measurements with YCSB's HdrHistogram semantics and text output format.
//!
//! The benchmarking scripts parse two things from YCSB output, so both are reproduced
//! exactly: the per-interval status summaries
//! (`[READ: Count=.., Max=.., Min=.., Avg=.., 90=.., 99=.., 99.9=.., 99.99=..]`) and the
//! final `TextMeasurementsExporter` lines (`[READ], AverageLatency(us), 595.26`).

use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, RwLock};

use anyhow::{bail, Result};
use hdrhistogram::Histogram;

use crate::props::Properties;
use crate::status::Status;
use crate::utils::{decimal_format_2, java_double_to_string, java_hash_map_order};

pub const MEASUREMENT_TYPE_PROPERTY: &str = "measurementtype";
pub const MEASUREMENT_INTERVAL_PROPERTY: &str = "measurement.interval";
pub const PERCENTILES_PROPERTY: &str = "hdrhistogram.percentiles";
const PERCENTILES_PROPERTY_DEFAULT: &str = "95,99";
const VERBOSE_PROPERTY: &str = "measurement.histogram.verbose";

/// Which latency a measurement records (`measurement.interval`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntervalMode {
    /// Service time: from issuing the operation to completion.
    Op,
    /// Response time from the throttled schedule's intended start, which includes any
    /// time an operation spent waiting behind a slow predecessor.
    Intended,
    Both,
}

pub struct Measurements {
    interval_mode: IntervalMode,
    percentiles: Vec<f64>,
    verbose: bool,
    ops: RwLock<MeasurementMap>,
    intended: RwLock<MeasurementMap>,
}

/// Measurements keyed by operation name, remembering insertion order.
#[derive(Default)]
struct MeasurementMap {
    entries: Vec<(String, Arc<OneMeasurement>)>,
    index: HashMap<String, usize>,
}

struct OneMeasurement {
    name: String,
    interval: Mutex<Histogram<u64>>,
    total: Mutex<Histogram<u64>>,
    return_codes: Mutex<Vec<(Status, u64)>>,
}

fn new_histogram() -> Histogram<u64> {
    // Same layout as Java's `new Recorder(3)`: auto-resizing, 3 significant digits.
    Histogram::new(3).expect("3 significant digits is a valid histogram configuration")
}

impl OneMeasurement {
    fn new(name: String) -> Self {
        Self {
            name,
            interval: Mutex::new(new_histogram()),
            total: Mutex::new(new_histogram()),
            return_codes: Mutex::new(Vec::new()),
        }
    }

    fn measure(&self, latency_us: u64) {
        let mut h = self.interval.lock().expect("histogram lock poisoned");
        // `record` grows the auto-resizing histogram; `saturating_record` would instead clamp
        // values above the current range, so it is only a fallback for absurd values.
        if h.record(latency_us).is_err() {
            h.saturating_record(latency_us);
        }
    }

    fn report_status(&self, status: Status) {
        let mut codes = self.return_codes.lock().expect("return code lock poisoned");
        match codes.iter_mut().find(|(s, _)| *s == status) {
            Some((_, count)) => *count += 1,
            None => codes.push((status, 1)),
        }
    }

    /// Swaps out the interval histogram and folds it into the running total, like
    /// `OneMeasurementHdrHistogram.getIntervalHistogramAndAccumulate`.
    fn take_interval(&self) -> Histogram<u64> {
        let interval = std::mem::replace(
            &mut *self.interval.lock().expect("histogram lock poisoned"),
            new_histogram(),
        );
        self.total
            .lock()
            .expect("histogram lock poisoned")
            .add(&interval)
            .expect("auto-resizing histograms always accept additions");
        interval
    }

    fn summary(&self) -> String {
        let h = self.take_interval();
        format!(
            "[{}: Count={}, Max={}, Min={}, Avg={}, 90={}, 99={}, 99.9={}, 99.99={}]",
            self.name,
            h.len(),
            h.max(),
            h.min(),
            decimal_format_2(h.mean()),
            java_value_at_percentile(&h, 90.0),
            java_value_at_percentile(&h, 99.0),
            java_value_at_percentile(&h, 99.9),
            java_value_at_percentile(&h, 99.99),
        )
    }

    fn export(&self, out: &mut dyn Write, percentiles: &[f64], verbose: bool) -> io::Result<()> {
        self.take_interval();
        let total = self.total.lock().expect("histogram lock poisoned");
        let name = &self.name;
        writeln!(out, "[{name}], Operations, {}", total.len())?;
        writeln!(
            out,
            "[{name}], AverageLatency(us), {}",
            java_double_to_string(total.mean())
        )?;
        writeln!(out, "[{name}], MinLatency(us), {}", total.min())?;
        writeln!(out, "[{name}], MaxLatency(us), {}", total.max())?;
        for &p in percentiles {
            writeln!(
                out,
                "[{name}], {}PercentileLatency(us), {}",
                ordinal(p),
                java_value_at_percentile(&total, p)
            )?;
        }
        for (status, count) in self.return_codes.lock().expect("return code lock poisoned").iter() {
            writeln!(out, "[{name}], Return={}, {count}", status.name())?;
        }
        if verbose {
            for v in total.iter_recorded() {
                let value = v.value_iterated_to().min(i32::MAX as u64);
                writeln!(
                    out,
                    "[{name}], {value}, {}",
                    java_double_to_string(v.count_at_value() as f64)
                )?;
            }
        }
        Ok(())
    }
}

impl Measurements {
    pub fn from_properties(props: &Properties) -> Result<Self> {
        let mtype = props.get_or(MEASUREMENT_TYPE_PROPERTY, "hdrhistogram");
        match mtype {
            "hdrhistogram" => {}
            "histogram" | "hdrhistogram+histogram" | "hdrhistogram+raw" | "timeseries" | "raw" => {
                eprintln!(
                    "[WARN] {MEASUREMENT_TYPE_PROPERTY}={mtype} is not implemented by the Rust client; using hdrhistogram."
                );
            }
            other => bail!("unknown {MEASUREMENT_TYPE_PROPERTY}={other}"),
        }
        let interval_mode = match props.get_or(MEASUREMENT_INTERVAL_PROPERTY, "op") {
            "op" => IntervalMode::Op,
            "intended" => IntervalMode::Intended,
            "both" => IntervalMode::Both,
            other => bail!("unknown {MEASUREMENT_INTERVAL_PROPERTY}={other}"),
        };
        let raw = props.get_or(PERCENTILES_PROPERTY, PERCENTILES_PROPERTY_DEFAULT);
        let percentiles = parse_percentiles(raw).unwrap_or_else(|| {
            eprintln!(
                "[WARN] Couldn't read {PERCENTILES_PROPERTY} value: '{raw}', the default of '{PERCENTILES_PROPERTY_DEFAULT}' will be used."
            );
            parse_percentiles(PERCENTILES_PROPERTY_DEFAULT).expect("default percentiles parse")
        });
        Ok(Self {
            interval_mode,
            percentiles,
            verbose: props.parse_bool(VERBOSE_PROPERTY, false),
            ops: RwLock::new(MeasurementMap::default()),
            intended: RwLock::new(MeasurementMap::default()),
        })
    }

    pub fn interval_mode(&self) -> IntervalMode {
        self.interval_mode
    }

    /// Records an operation latency in microseconds (`Measurements.measure`).
    pub fn measure(&self, operation: &str, latency_us: u64) {
        if self.interval_mode == IntervalMode::Intended {
            return;
        }
        self.op_measurement(operation).measure(latency_us);
    }

    /// Records a latency measured from the intended start time (`measureIntended`).
    pub fn measure_intended(&self, operation: &str, latency_us: u64) {
        if self.interval_mode == IntervalMode::Op {
            return;
        }
        self.intended_measurement(operation).measure(latency_us);
    }

    pub fn report_status(&self, operation: &str, status: Status) {
        let m = if self.interval_mode == IntervalMode::Intended {
            self.intended_measurement(operation)
        } else {
            self.op_measurement(operation)
        };
        m.report_status(status);
    }

    /// One status-line summary for every measurement, swapping interval histograms.
    pub fn summary(&self) -> String {
        let mut out = String::new();
        for m in self.ordered(&self.ops).into_iter().chain(self.ordered(&self.intended)) {
            out.push_str(&m.summary());
            out.push(' ');
        }
        out
    }

    pub fn export(&self, out: &mut dyn Write) -> io::Result<()> {
        for m in self.ordered(&self.ops).into_iter().chain(self.ordered(&self.intended)) {
            m.export(out, &self.percentiles, self.verbose)?;
        }
        Ok(())
    }

    fn op_measurement(&self, operation: &str) -> Arc<OneMeasurement> {
        Self::get_or_insert(&self.ops, operation, || operation.to_string())
    }

    fn intended_measurement(&self, operation: &str) -> Arc<OneMeasurement> {
        let mode = self.interval_mode;
        Self::get_or_insert(&self.intended, operation, || {
            if mode == IntervalMode::Intended {
                operation.to_string()
            } else {
                format!("Intended-{operation}")
            }
        })
    }

    fn get_or_insert(
        map: &RwLock<MeasurementMap>,
        operation: &str,
        name: impl FnOnce() -> String,
    ) -> Arc<OneMeasurement> {
        {
            let r = map.read().expect("measurement map lock poisoned");
            if let Some(&i) = r.index.get(operation) {
                return Arc::clone(&r.entries[i].1);
            }
        }
        let mut w = map.write().expect("measurement map lock poisoned");
        if let Some(&i) = w.index.get(operation) {
            return Arc::clone(&w.entries[i].1);
        }
        let m = Arc::new(OneMeasurement::new(name()));
        let position = w.entries.len();
        w.index.insert(operation.to_string(), position);
        w.entries.push((operation.to_string(), Arc::clone(&m)));
        m
    }

    /// Entries in the order Java's `ConcurrentHashMap` would iterate them.
    fn ordered(&self, map: &RwLock<MeasurementMap>) -> Vec<Arc<OneMeasurement>> {
        let mut entries = map.read().expect("measurement map lock poisoned").entries.clone();
        java_hash_map_order(&mut entries, |(key, _)| key.as_str());
        entries.into_iter().map(|(_, m)| m).collect()
    }
}

fn parse_percentiles(raw: &str) -> Option<Vec<f64>> {
    raw.split(',').map(crate::props::parse_java_double).collect()
}

/// `OneMeasurementHdrHistogram.ordinal`: `95` -> `95th`, `99.9` -> `99.9`.
fn ordinal(p: f64) -> String {
    if p.fract() == 0.0 {
        let j = p as i64;
        let suffix = match j % 100 {
            11..=13 => "th",
            _ => ["th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th"][(j % 10) as usize],
        };
        format!("{j}{suffix}")
    } else {
        java_double_to_string(p)
    }
}

/// `AbstractHistogram.getValueAtPercentile` from HdrHistogram Java, including its
/// one-ulp nudge below the requested percentile.
pub fn java_value_at_percentile(h: &Histogram<u64>, percentile: f64) -> u64 {
    let total = h.len();
    if total == 0 {
        return 0;
    }
    let requested = percentile.next_down().clamp(0.0, 100.0);
    let count_at_percentile = (((requested * total as f64) / 100.0).ceil() as u64).max(1);
    let mut cumulative = 0u64;
    for v in h.iter_recorded() {
        cumulative += v.count_at_value();
        if cumulative >= count_at_percentile {
            let value = v.value_iterated_to();
            return if percentile == 0.0 {
                h.lowest_equivalent(value)
            } else {
                value
            };
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn measurements(extra: &str) -> Measurements {
        let mut p = Properties::new();
        p.load_str(extra);
        Measurements::from_properties(&p).unwrap()
    }

    #[test]
    fn summary_formats_like_ycsb_hdrhistogram() {
        let m = measurements("");
        for v in [1000, 2000, 3000, 4000] {
            m.measure("READ", v);
        }
        let s = m.summary();
        assert_eq!(
            s,
            "[READ: Count=4, Max=4001, Min=1000, Avg=2500.5, 90=4001, 99=4001, 99.9=4001, 99.99=4001] "
        );
        // The interval was drained into the total, so the next interval is empty.
        assert_eq!(
            m.summary(),
            "[READ: Count=0, Max=0, Min=0, Avg=0, 90=0, 99=0, 99.9=0, 99.99=0] "
        );
    }

    #[test]
    fn export_writes_text_exporter_lines_in_java_order() {
        let m = measurements("");
        m.measure("UPDATE", 500);
        m.report_status("UPDATE", Status::Ok);
        m.measure("READ", 100);
        m.report_status("READ", Status::Ok);
        m.measure("READ-FAILED", 300);
        m.report_status("READ", Status::NotFound);
        m.measure("CLEANUP", 2);
        let mut out = Vec::new();
        m.export(&mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        let headers: Vec<&str> = text.lines().filter(|l| l.contains(", Operations, ")).collect();
        assert_eq!(
            headers,
            vec![
                "[READ], Operations, 1",
                "[CLEANUP], Operations, 1",
                "[UPDATE], Operations, 1",
                "[READ-FAILED], Operations, 1",
            ]
        );
        assert!(text.contains("[READ], AverageLatency(us), 100.0\n"));
        assert!(text.contains("[READ], MinLatency(us), 100\n"));
        assert!(text.contains("[READ], MaxLatency(us), 100\n"));
        assert!(text.contains("[READ], 95thPercentileLatency(us), 100\n"));
        assert!(text.contains("[READ], 99thPercentileLatency(us), 100\n"));
        assert!(text.contains("[READ], Return=OK, 1\n[READ], Return=NOT_FOUND, 1\n"));
        assert!(text.contains("[UPDATE], Return=OK, 1\n"));
    }

    #[test]
    fn export_includes_values_recorded_before_and_after_summaries() {
        let m = measurements("");
        m.measure("INSERT", 10);
        let _ = m.summary();
        m.measure("INSERT", 20);
        let mut out = Vec::new();
        m.export(&mut out).unwrap();
        assert!(String::from_utf8(out).unwrap().contains("[INSERT], Operations, 2\n"));
    }

    #[test]
    fn intended_mode_records_under_plain_names() {
        let m = measurements("measurement.interval=intended");
        m.measure("READ", 10);
        m.measure_intended("READ", 20);
        let mut out = Vec::new();
        m.export(&mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("[READ], Operations, 1\n"));
        assert!(text.contains("[READ], AverageLatency(us), 20.0\n"));
    }

    #[test]
    fn both_mode_prefixes_intended_measurements() {
        let m = measurements("measurement.interval=both");
        m.measure("READ", 10);
        m.measure_intended("READ", 20);
        let mut out = Vec::new();
        m.export(&mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("[READ], Operations, 1\n"));
        assert!(text.contains("[Intended-READ], Operations, 1\n"));
    }

    #[test]
    fn custom_percentiles_use_ordinals() {
        let m = measurements("hdrhistogram.percentiles=50,99.9");
        m.measure("READ", 10);
        let mut out = Vec::new();
        m.export(&mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("[READ], 50thPercentileLatency(us), 10\n"));
        assert!(text.contains("[READ], 99.9PercentileLatency(us), 10\n"));
    }

    #[test]
    fn ordinal_matches_java() {
        assert_eq!(ordinal(95.0), "95th");
        assert_eq!(ordinal(99.0), "99th");
        assert_eq!(ordinal(1.0), "1st");
        assert_eq!(ordinal(2.0), "2nd");
        assert_eq!(ordinal(3.0), "3rd");
        assert_eq!(ordinal(11.0), "11th");
        assert_eq!(ordinal(99.9), "99.9");
    }

    #[test]
    fn java_value_at_percentile_matches_hdrhistogram_java() {
        let mut h = new_histogram();
        for v in 1..=100u64 {
            h.record(v).unwrap();
        }
        assert_eq!(java_value_at_percentile(&h, 50.0), 50);
        assert_eq!(java_value_at_percentile(&h, 90.0), 90);
        assert_eq!(java_value_at_percentile(&h, 99.0), 99);
        assert_eq!(java_value_at_percentile(&h, 100.0), 100);
        assert_eq!(java_value_at_percentile(&h, 0.0), 1);
        assert_eq!(java_value_at_percentile(&new_histogram(), 99.0), 0);
    }

    #[test]
    fn unknown_measurement_type_is_rejected() {
        let mut p = Properties::new();
        p.set("measurementtype", "bogus");
        assert!(Measurements::from_properties(&p).is_err());
    }

    #[test]
    fn measure_is_safe_across_threads() {
        let m = Arc::new(measurements(""));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let m = Arc::clone(&m);
                std::thread::spawn(move || {
                    for i in 0..10_000 {
                        m.measure("READ", i % 1000 + 1);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let mut out = Vec::new();
        m.export(&mut out).unwrap();
        assert!(String::from_utf8(out).unwrap().contains("[READ], Operations, 80000\n"));
    }
}
