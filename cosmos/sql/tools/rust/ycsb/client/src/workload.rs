//! Port of `site.ycsb.workloads.CoreWorkload`.
//!
//! Key names, value sizes, the operation mix and the request distributions follow the Java
//! implementation, so a dataset loaded by either client can be run by the other and the
//! workload files (`workloada`..`workloadf`) mean the same thing.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::db::{monotonic_nanos, Db, DbWrapper, Record, Values};
use crate::generator::{
    AcknowledgedCounterGenerator, ConstantIntegerGenerator, CounterGenerator, DiscreteGenerator, ExponentialGenerator,
    HistogramGenerator, HotspotIntegerGenerator, NumberGenerator, ScrambledZipfianGenerator, SequentialGenerator,
    SkewedLatestGenerator, UniformLongGenerator, ZipfianGenerator,
};
use crate::measurements::Measurements;
use crate::props::Properties;
use crate::status::Status;
use crate::utils::{fnvhash64, java_string_hash};

pub const WORKLOAD_PROPERTY: &str = "workload";
pub const CORE_WORKLOAD_CLASS: &str = "site.ycsb.workloads.CoreWorkload";
pub const RECORD_COUNT_PROPERTY: &str = "recordcount";
pub const DEFAULT_RECORD_COUNT: &str = "0";
pub const OPERATION_COUNT_PROPERTY: &str = "operationcount";
pub const INSERT_COUNT_PROPERTY: &str = "insertcount";
pub const INSERT_START_PROPERTY: &str = "insertstart";

const INT_MAX: i64 = i32::MAX as i64;

pub struct CoreWorkload {
    table: String,
    field_names: Vec<String>,
    field_length_generator: Box<dyn NumberGenerator>,
    read_all_fields: bool,
    read_all_fields_by_name: bool,
    write_all_fields: bool,
    data_integrity: bool,
    key_sequence: CounterGenerator,
    operation_chooser: DiscreteGenerator,
    key_chooser: Box<dyn NumberGenerator>,
    key_chooser_is_exponential: bool,
    field_chooser: UniformLongGenerator,
    transaction_insert_key_sequence: Arc<AcknowledgedCounterGenerator>,
    scan_length: Box<dyn NumberGenerator>,
    ordered_inserts: bool,
    zero_padding: i32,
    insertion_retry_limit: i32,
    insertion_retry_interval: i32,
    measurements: Arc<Measurements>,
    stop_requested: AtomicBool,
}

impl CoreWorkload {
    pub fn new(p: &Properties, measurements: Arc<Measurements>) -> Result<Self> {
        let table = p.get_or("table", "usertable").to_string();
        let field_count = p.parse_i64("fieldcount", "10")?;
        let field_name_prefix = p.get_or("fieldnameprefix", "field");
        let field_names: Vec<String> = (0..field_count).map(|i| format!("{field_name_prefix}{i}")).collect();
        let field_length_generator = field_length_generator(p)?;

        let mut record_count = p.parse_i64(RECORD_COUNT_PROPERTY, DEFAULT_RECORD_COUNT)?;
        if record_count == 0 {
            record_count = INT_MAX;
        }
        let request_distribution = p.get_or("requestdistribution", "uniform");
        let min_scan_length = p.parse_i32("minscanlength", "1")?;
        let max_scan_length = p.parse_i32("maxscanlength", "1000")?;
        let scan_length_distribution = p.get_or("scanlengthdistribution", "uniform");

        let insert_start = p.parse_i64(INSERT_START_PROPERTY, "0")?;
        let insert_count = p.parse_i64(INSERT_COUNT_PROPERTY, &(record_count - insert_start).to_string())?;
        if record_count < insert_start + insert_count {
            bail!(
                "Invalid combination of insertstart, insertcount and recordcount.\n\
                 recordcount must be bigger than insertstart + insertcount."
            );
        }
        let zero_padding = p.parse_i32("zeropadding", "1")?;
        let read_all_fields = p.parse_bool("readallfields", true);
        let read_all_fields_by_name = p.parse_bool("readallfieldsbyname", false);
        let write_all_fields = p.parse_bool("writeallfields", false);
        let data_integrity = p.parse_bool("dataintegrity", false);
        if data_integrity && p.get_or("fieldlengthdistribution", "constant") != "constant" {
            bail!("Must have constant field size to check data integrity.");
        }
        if data_integrity {
            println!("Data integrity is enabled.");
        }
        let ordered_inserts = p.get_or("insertorder", "hashed") != "hashed";

        let transaction_insert_key_sequence = Arc::new(AcknowledgedCounterGenerator::new(record_count));
        let mut key_chooser_is_exponential = false;
        let key_chooser: Box<dyn NumberGenerator> = match request_distribution {
            "uniform" => Box::new(UniformLongGenerator::new(insert_start, insert_start + insert_count - 1)),
            "exponential" => {
                key_chooser_is_exponential = true;
                let percentile = p.parse_f64(
                    ExponentialGenerator::PERCENTILE_PROPERTY,
                    ExponentialGenerator::PERCENTILE_DEFAULT,
                )?;
                let frac = p.parse_f64(ExponentialGenerator::FRAC_PROPERTY, ExponentialGenerator::FRAC_DEFAULT)?;
                Box::new(ExponentialGenerator::new(percentile, record_count as f64 * frac))
            }
            "sequential" => Box::new(SequentialGenerator::new(insert_start, insert_start + insert_count - 1)),
            "zipfian" => {
                // Leave room in the key space for keys inserted during the run so the set of
                // popular keys stays stable as the table grows (2 is YCSB's fudge factor).
                let insert_proportion = p.parse_f64("insertproportion", "0.0")?;
                let op_count = p
                    .get(OPERATION_COUNT_PROPERTY)
                    .and_then(crate::props::parse_java_int)
                    .context("zipfian request distribution needs an integer operationcount")?;
                let expected_new_keys = (f64::from(op_count) * insert_proportion * 2.0) as i32;
                Box::new(ScrambledZipfianGenerator::new(
                    insert_start,
                    insert_start + insert_count + i64::from(expected_new_keys),
                ))
            }
            "latest" => Box::new(SkewedLatestGenerator::new(Arc::clone(&transaction_insert_key_sequence))),
            "hotspot" => {
                let hot_set_fraction = p.parse_f64("hotspotdatafraction", "0.2")?;
                let hot_op_fraction = p.parse_f64("hotspotopnfraction", "0.8")?;
                Box::new(HotspotIntegerGenerator::new(
                    insert_start,
                    insert_start + insert_count - 1,
                    hot_set_fraction,
                    hot_op_fraction,
                ))
            }
            other => bail!("Unknown request distribution \"{other}\""),
        };

        let scan_length: Box<dyn NumberGenerator> = match scan_length_distribution {
            "uniform" => Box::new(UniformLongGenerator::new(
                i64::from(min_scan_length),
                i64::from(max_scan_length),
            )),
            "zipfian" => Box::new(ZipfianGenerator::new(
                i64::from(min_scan_length),
                i64::from(max_scan_length),
            )),
            other => bail!("Distribution \"{other}\" not allowed for scan length"),
        };

        Ok(Self {
            table,
            field_length_generator,
            read_all_fields,
            read_all_fields_by_name,
            write_all_fields,
            data_integrity,
            key_sequence: CounterGenerator::new(insert_start),
            operation_chooser: operation_generator(p)?,
            key_chooser,
            key_chooser_is_exponential,
            field_chooser: UniformLongGenerator::new(0, field_count - 1),
            transaction_insert_key_sequence,
            scan_length,
            ordered_inserts,
            zero_padding,
            insertion_retry_limit: p.parse_i32("core_workload_insertion_retry_limit", "0")?,
            insertion_retry_interval: p.parse_i32("core_workload_insertion_retry_interval", "3")?,
            measurements,
            stop_requested: AtomicBool::new(false),
            field_names,
        })
    }

    pub fn request_stop(&self) {
        self.stop_requested.store(true, Ordering::Relaxed);
    }

    pub fn is_stop_requested(&self) -> bool {
        self.stop_requested.load(Ordering::Relaxed)
    }

    /// One load-phase insert (`doInsert`). Returns false when the insert ultimately failed,
    /// which ends the calling client task.
    pub async fn do_insert<D: Db>(&self, db: &DbWrapper<D>) -> bool {
        let key_num = i64::from(self.key_sequence.next() as i32);
        let key = build_key_name(key_num, self.zero_padding, self.ordered_inserts);
        let values = self.build_values(&key);
        let mut retries = 0;
        loop {
            let status = db.insert(&self.table, &key, &values).await;
            if status.is_ok() {
                return true;
            }
            retries += 1;
            if retries <= self.insertion_retry_limit {
                eprintln!("Retrying insertion, retry count: {retries}");
                // Sleep for a random duration in [0.8, 1.2) * retry interval.
                let factor = 0.8 + 0.4 * crate::rng::next_f64();
                let millis = (1000.0 * f64::from(self.insertion_retry_interval) * factor) as u64;
                tokio::time::sleep(Duration::from_millis(millis)).await;
            } else {
                eprintln!(
                    "Error inserting, not retrying any more. number of attempts: {retries}Insertion Retry Limit: {}",
                    self.insertion_retry_limit
                );
                return false;
            }
        }
    }

    /// One run-phase operation (`doTransaction`). `Ok(false)` stops the calling client task.
    pub async fn do_transaction<D: Db>(&self, db: &DbWrapper<D>) -> Result<bool> {
        let Some(operation) = self.operation_chooser.next_string() else {
            return Ok(false);
        };
        match operation {
            "READ" => self.do_transaction_read(db).await,
            "UPDATE" => self.do_transaction_update(db).await,
            "INSERT" => self.do_transaction_insert(db).await?,
            "SCAN" => self.do_transaction_scan(db).await,
            _ => self.do_transaction_read_modify_write(db).await,
        }
        Ok(true)
    }

    fn next_key_num(&self) -> i64 {
        if self.key_chooser_is_exponential {
            loop {
                let key_num = self.transaction_insert_key_sequence.last_value()
                    - i64::from(self.key_chooser.next_value().int_value());
                if key_num >= 0 {
                    return key_num;
                }
            }
        }
        loop {
            let key_num = i64::from(self.key_chooser.next_value().int_value());
            if key_num <= self.transaction_insert_key_sequence.last_value() {
                return key_num;
            }
        }
    }

    fn random_field(&self) -> &str {
        &self.field_names[self.field_chooser.next() as i32 as usize]
    }

    fn single_field_set(&self) -> HashSet<String> {
        HashSet::from([self.random_field().to_string()])
    }

    async fn do_transaction_read<D: Db>(&self, db: &DbWrapper<D>) {
        let key = build_key_name(self.next_key_num(), self.zero_padding, self.ordered_inserts);
        let fields = if !self.read_all_fields {
            Some(self.single_field_set())
        } else if self.data_integrity || self.read_all_fields_by_name {
            Some(self.field_names.iter().cloned().collect())
        } else {
            None
        };
        let mut cells = Record::new();
        db.read(&self.table, &key, fields.as_ref(), &mut cells).await;
        if self.data_integrity {
            self.verify_row(&key, &cells);
        }
    }

    async fn do_transaction_read_modify_write<D: Db>(&self, db: &DbWrapper<D>) {
        let key = build_key_name(self.next_key_num(), self.zero_padding, self.ordered_inserts);
        let fields = (!self.read_all_fields).then(|| self.single_field_set());
        let values = if self.write_all_fields {
            self.build_values(&key)
        } else {
            self.build_single_value(&key)
        };
        let mut cells = Record::new();
        let ist = db.intended_start_ns();
        let st = monotonic_nanos();
        db.read(&self.table, &key, fields.as_ref(), &mut cells).await;
        db.update(&self.table, &key, &values).await;
        let en = monotonic_nanos();
        if self.data_integrity {
            self.verify_row(&key, &cells);
        }
        self.measurements
            .measure("READ-MODIFY-WRITE", en.saturating_sub(st) / 1000);
        self.measurements
            .measure_intended("READ-MODIFY-WRITE", en.saturating_sub(ist) / 1000);
    }

    async fn do_transaction_scan<D: Db>(&self, db: &DbWrapper<D>) {
        let start_key = build_key_name(self.next_key_num(), self.zero_padding, self.ordered_inserts);
        let len = self.scan_length.next_value().int_value();
        let fields = (!self.read_all_fields).then(|| self.single_field_set());
        let mut result = Vec::new();
        db.scan(&self.table, &start_key, len, fields.as_ref(), &mut result)
            .await;
    }

    async fn do_transaction_update<D: Db>(&self, db: &DbWrapper<D>) {
        let key = build_key_name(self.next_key_num(), self.zero_padding, self.ordered_inserts);
        let values = if self.write_all_fields {
            self.build_values(&key)
        } else {
            self.build_single_value(&key)
        };
        db.update(&self.table, &key, &values).await;
    }

    async fn do_transaction_insert<D: Db>(&self, db: &DbWrapper<D>) -> Result<()> {
        let key_num = self.transaction_insert_key_sequence.next();
        let key = build_key_name(key_num, self.zero_padding, self.ordered_inserts);
        let values = self.build_values(&key);
        db.insert(&self.table, &key, &values).await;
        self.transaction_insert_key_sequence.acknowledge(key_num)
    }

    fn verify_row(&self, key: &str, cells: &Record) {
        let start = monotonic_nanos();
        let status = if cells.is_empty() {
            Status::Error
        } else if cells
            .iter()
            .any(|(field, value)| *value != self.build_deterministic_value(key, field))
        {
            Status::UnexpectedState
        } else {
            Status::Ok
        };
        let end = monotonic_nanos();
        self.measurements.measure("VERIFY", end.saturating_sub(start) / 1000);
        self.measurements.report_status("VERIFY", status);
    }

    fn build_single_value(&self, key: &str) -> Values {
        let field = self.random_field().to_string();
        let data = self.field_value(key, &field);
        vec![(field, data)]
    }

    fn build_values(&self, key: &str) -> Values {
        self.field_names
            .iter()
            .map(|field| (field.clone(), self.field_value(key, field)))
            .collect()
    }

    fn field_value(&self, key: &str, field: &str) -> String {
        if self.data_integrity {
            self.build_deterministic_value(key, field)
        } else {
            random_field_value(self.field_length_generator.next_value().long_value().max(0) as usize)
        }
    }

    fn build_deterministic_value(&self, key: &str, field: &str) -> String {
        let size = self.field_length_generator.next_value().int_value().max(0) as usize;
        let mut sb = format!("{key}:{field}");
        while sb.len() < size {
            let hash = java_string_hash(&sb);
            sb.push(':');
            sb.push_str(&hash.to_string());
        }
        sb.truncate(size);
        sb
    }
}

/// `CoreWorkload.buildKeyName`: `user` + (hashed or ordered) key number, zero padded.
pub fn build_key_name(key_num: i64, zero_padding: i32, ordered_inserts: bool) -> String {
    let key_num = if ordered_inserts { key_num } else { fnvhash64(key_num) };
    let value = key_num.to_string();
    let fill = (zero_padding as i64 - value.len() as i64).max(0) as usize;
    let mut key = String::with_capacity(4 + fill + value.len());
    key.push_str("user");
    key.extend(std::iter::repeat_n('0', fill));
    key.push_str(&value);
    key
}

/// Random printable value with the character distribution of YCSB's `RandomByteIterator`.
///
/// Each 32-bit draw yields six characters; the alternating 5/6/7-bit masks produce ASCII in
/// 32..=127, so JSON-escaped document sizes match what the Java client writes.
pub fn random_field_value(len: usize) -> String {
    const MASKS: [u32; 6] = [31, 63, 95, 31, 63, 95];
    let mut bytes = Vec::with_capacity(len);
    while bytes.len() < len {
        let draw = crate::rng::next_i32() as u32;
        let chunk = (len - bytes.len()).min(6);
        for (pos, mask) in MASKS.iter().enumerate().take(chunk) {
            bytes.push(((draw >> (5 * pos)) & mask) as u8 + b' ');
        }
    }
    String::from_utf8(bytes).expect("generated bytes are ASCII")
}

fn field_length_generator(p: &Properties) -> Result<Box<dyn NumberGenerator>> {
    let distribution = p.get_or("fieldlengthdistribution", "constant");
    let field_length = p.parse_i32("fieldlength", "100")?;
    let min_field_length = p.parse_i32("minfieldlength", "1")?;
    let histogram_file = p.get_or("fieldlengthhistogram", "hist.txt");
    Ok(match distribution {
        "constant" => Box::new(ConstantIntegerGenerator::new(field_length)),
        "uniform" => Box::new(UniformLongGenerator::new(
            i64::from(min_field_length),
            i64::from(field_length),
        )),
        "zipfian" => Box::new(ZipfianGenerator::new(
            i64::from(min_field_length),
            i64::from(field_length),
        )),
        "histogram" => Box::new(
            HistogramGenerator::from_file(histogram_file)
                .with_context(|| format!("Couldn't read field length histogram file: {histogram_file}"))?,
        ),
        other => bail!("Unknown field length distribution \"{other}\""),
    })
}

fn operation_generator(p: &Properties) -> Result<DiscreteGenerator> {
    let mut chooser = DiscreteGenerator::default();
    for (property, default, op) in [
        ("readproportion", "0.95", "READ"),
        ("updateproportion", "0.05", "UPDATE"),
        ("insertproportion", "0.0", "INSERT"),
        ("scanproportion", "0.0", "SCAN"),
        ("readmodifywriteproportion", "0.0", "READMODIFYWRITE"),
    ] {
        let proportion = p.parse_f64(property, default)?;
        if proportion > 0.0 {
            chooser.add_value(proportion, op);
        }
    }
    Ok(chooser)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Record;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[test]
    fn build_key_name_matches_java() {
        assert_eq!(build_key_name(0, 1, false), "user6284781860667377211");
        assert_eq!(build_key_name(1, 1, false), "user8517097267634966620");
        assert_eq!(build_key_name(7, 1, true), "user7");
        assert_eq!(build_key_name(7, 5, true), "user00007");
    }

    #[test]
    fn random_field_value_has_requested_length_and_printable_range() {
        for len in [0, 1, 5, 6, 7, 100] {
            let v = random_field_value(len);
            assert_eq!(v.len(), len);
            assert!(v.bytes().all(|b| (32..=127).contains(&b)), "{v:?}");
        }
    }

    #[test]
    fn random_field_value_uses_ycsb_character_classes() {
        // Position 0 of each 6-char chunk is masked to 5 bits: characters 32..=63 only.
        for _ in 0..1000 {
            let v = random_field_value(6);
            let b = v.as_bytes();
            assert!((32..=63).contains(&b[0]));
            assert!((32..=95).contains(&b[1]));
            assert!((32..=63).contains(&b[2]) || (96..=127).contains(&b[2]));
        }
    }

    /// Records every call so tests can check what the workload asked the binding to do.
    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<(String, String, usize)>>,
        insert_failures_left: Mutex<u32>,
        stored: Mutex<HashMap<String, Values>>,
    }

    impl Db for Arc<Recorder> {
        async fn init(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn cleanup(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn read(&self, _: &str, key: &str, fields: Option<&HashSet<String>>, result: &mut Record) -> Status {
            self.calls
                .lock()
                .unwrap()
                .push(("READ".into(), key.into(), fields.map_or(0, |f| f.len())));
            if let Some(values) = self.stored.lock().unwrap().get(key) {
                for (f, v) in values {
                    if fields.is_none_or(|set| set.contains(f)) {
                        result.insert(f.clone(), v.clone());
                    }
                }
            }
            Status::Ok
        }
        async fn scan(&self, _: &str, key: &str, n: i32, _: Option<&HashSet<String>>, _: &mut Vec<Record>) -> Status {
            self.calls.lock().unwrap().push(("SCAN".into(), key.into(), n as usize));
            Status::Ok
        }
        async fn update(&self, _: &str, key: &str, values: &Values) -> Status {
            self.calls
                .lock()
                .unwrap()
                .push(("UPDATE".into(), key.into(), values.len()));
            Status::Ok
        }
        async fn insert(&self, _: &str, key: &str, values: &Values) -> Status {
            self.calls
                .lock()
                .unwrap()
                .push(("INSERT".into(), key.into(), values.len()));
            let mut left = self.insert_failures_left.lock().unwrap();
            if *left > 0 {
                *left -= 1;
                return Status::Error;
            }
            self.stored.lock().unwrap().insert(key.into(), values.clone());
            Status::Ok
        }
        async fn delete(&self, _: &str, _: &str) -> Status {
            Status::Ok
        }
    }

    fn setup(props: &str) -> (CoreWorkload, DbWrapper<Arc<Recorder>>, Arc<Recorder>) {
        let mut p = Properties::new();
        p.load_str(props);
        let m = Arc::new(Measurements::from_properties(&p).unwrap());
        let w = CoreWorkload::new(&p, Arc::clone(&m)).unwrap();
        let rec = Arc::new(Recorder::default());
        let db = DbWrapper::new(Arc::clone(&rec), m, &p);
        (w, db, rec)
    }

    #[tokio::test]
    async fn do_insert_writes_sequential_hashed_keys_with_all_fields() {
        let (w, db, rec) = setup("recordcount=10\nfieldcount=3\nfieldlength=8\n");
        for _ in 0..3 {
            assert!(w.do_insert(&db).await);
        }
        let calls = rec.calls.lock().unwrap();
        let keys: Vec<&str> = calls.iter().map(|c| c.1.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "user6284781860667377211",
                "user8517097267634966620",
                "user1820151046732198393"
            ]
        );
        assert!(calls.iter().all(|c| c.0 == "INSERT" && c.2 == 3));
        let stored = rec.stored.lock().unwrap();
        let values = &stored["user6284781860667377211"];
        assert_eq!(
            values.iter().map(|(f, _)| f.as_str()).collect::<Vec<_>>(),
            vec!["field0", "field1", "field2"]
        );
        assert!(values.iter().all(|(_, v)| v.len() == 8));
    }

    #[tokio::test]
    async fn do_insert_respects_insertstart() {
        let (w, db, rec) = setup("recordcount=10\ninsertstart=5\ninsertcount=2\ninsertorder=ordered\n");
        w.do_insert(&db).await;
        w.do_insert(&db).await;
        let keys: Vec<String> = rec.calls.lock().unwrap().iter().map(|c| c.1.clone()).collect();
        assert_eq!(keys, vec!["user5", "user6"]);
    }

    #[tokio::test(start_paused = true)]
    async fn do_insert_retries_up_to_the_limit() {
        let (w, db, rec) = setup("recordcount=10\ncore_workload_insertion_retry_limit=2\n");
        *rec.insert_failures_left.lock().unwrap() = 2;
        assert!(w.do_insert(&db).await);
        assert_eq!(rec.calls.lock().unwrap().len(), 3);

        *rec.insert_failures_left.lock().unwrap() = 5;
        assert!(!w.do_insert(&db).await);
    }

    #[tokio::test]
    async fn do_transaction_follows_operation_mix() {
        let (w, db, rec) = setup(
            "recordcount=100\noperationcount=1000\nreadproportion=0.5\nupdateproportion=0.5\n\
             requestdistribution=zipfian\nreadallfields=true\n",
        );
        for _ in 0..2000 {
            assert!(w.do_transaction(&db).await.unwrap());
        }
        let calls = rec.calls.lock().unwrap();
        let reads = calls.iter().filter(|c| c.0 == "READ").count();
        let updates = calls.iter().filter(|c| c.0 == "UPDATE").count();
        assert_eq!(reads + updates, 2000);
        assert!((850..1150).contains(&reads), "{reads}");
        // Reads fetch all fields (no field set); updates write a single random field.
        assert!(calls.iter().filter(|c| c.0 == "READ").all(|c| c.2 == 0));
        assert!(calls.iter().filter(|c| c.0 == "UPDATE").all(|c| c.2 == 1));
        // Every chosen key must be one of the 100 loaded keys.
        let loaded: HashSet<String> = (0..100).map(|i| build_key_name(i, 1, false)).collect();
        assert!(calls.iter().all(|c| loaded.contains(&c.1)));
    }

    #[tokio::test]
    async fn uniform_keys_stay_within_insert_range() {
        let (w, db, rec) = setup(
            "recordcount=300\ninsertstart=100\ninsertcount=100\noperationcount=10\nreadproportion=1\n\
             updateproportion=0\nrequestdistribution=uniform\ninsertorder=ordered\n",
        );
        for _ in 0..500 {
            w.do_transaction(&db).await.unwrap();
        }
        for (_, key, _) in rec.calls.lock().unwrap().iter() {
            let n: i64 = key.trim_start_matches("user").parse().unwrap();
            assert!((100..200).contains(&n), "{key}");
        }
    }

    #[tokio::test]
    async fn scan_uses_scan_length_distribution() {
        let (w, db, rec) = setup(
            "recordcount=100\noperationcount=10\nreadproportion=0\nupdateproportion=0\nscanproportion=1\n\
             maxscanlength=10\nrequestdistribution=uniform\n",
        );
        for _ in 0..200 {
            w.do_transaction(&db).await.unwrap();
        }
        let calls = rec.calls.lock().unwrap();
        assert!(calls.iter().all(|c| c.0 == "SCAN" && (1..=10).contains(&c.2)));
    }

    #[tokio::test]
    async fn transaction_inserts_extend_the_key_space() {
        let (w, db, rec) = setup(
            "recordcount=10\noperationcount=10\nreadproportion=0\nupdateproportion=0\ninsertproportion=1\n\
             insertorder=ordered\n",
        );
        for _ in 0..3 {
            w.do_transaction(&db).await.unwrap();
        }
        let keys: Vec<String> = rec.calls.lock().unwrap().iter().map(|c| c.1.clone()).collect();
        assert_eq!(keys, vec!["user10", "user11", "user12"]);
        assert_eq!(w.transaction_insert_key_sequence.last_value(), 12);
    }

    #[tokio::test]
    async fn read_modify_write_reads_then_updates_and_is_measured() {
        let (w, db, rec) = setup(
            "recordcount=10\noperationcount=10\nreadproportion=0\nupdateproportion=0\n\
             readmodifywriteproportion=1\nrequestdistribution=uniform\n",
        );
        w.do_transaction(&db).await.unwrap();
        let calls = rec.calls.lock().unwrap();
        assert_eq!(
            calls.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(),
            vec!["READ", "UPDATE"]
        );
        assert_eq!(calls[0].1, calls[1].1);
        let mut out = Vec::new();
        db.measurements().export(&mut out).unwrap();
        assert!(String::from_utf8(out)
            .unwrap()
            .contains("[READ-MODIFY-WRITE], Operations, 1\n"));
    }

    #[tokio::test]
    async fn data_integrity_round_trips_deterministic_values() {
        let (w, db, _rec) = setup(
            "recordcount=5\noperationcount=10\nreadproportion=1\nupdateproportion=0\ndataintegrity=true\n\
             fieldlength=50\nrequestdistribution=uniform\n",
        );
        for _ in 0..5 {
            assert!(w.do_insert(&db).await);
        }
        for _ in 0..20 {
            w.do_transaction(&db).await.unwrap();
        }
        let mut out = Vec::new();
        db.measurements().export(&mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("[VERIFY], Return=OK, 20\n"), "{text}");
    }

    #[test]
    fn build_deterministic_value_matches_java_algorithm() {
        let mut p = Properties::new();
        p.load_str("fieldlength=40\n");
        let w = CoreWorkload::new(&p, Arc::new(Measurements::from_properties(&p).unwrap())).unwrap();
        let v = w.build_deterministic_value("user1", "field0");
        assert_eq!(v.len(), 40);
        let first_hash = java_string_hash("user1:field0");
        assert!(v.starts_with(&format!("user1:field0:{first_hash}")), "{v}");
    }

    #[test]
    fn invalid_insert_range_is_rejected() {
        let mut p = Properties::new();
        p.load_str("recordcount=10\ninsertstart=5\ninsertcount=10\n");
        assert!(CoreWorkload::new(&p, Arc::new(Measurements::from_properties(&p).unwrap())).is_err());
    }

    #[test]
    fn unknown_request_distribution_is_rejected() {
        let mut p = Properties::new();
        p.load_str("requestdistribution=bogus\n");
        assert!(CoreWorkload::new(&p, Arc::new(Measurements::from_properties(&p).unwrap())).is_err());
    }
}
