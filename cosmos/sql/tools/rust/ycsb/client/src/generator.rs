//! Ports of the `site.ycsb.generator` classes used by `CoreWorkload`.
//!
//! Generators are shared by every client task, so state that Java mutates lives in atomics
//! or behind a mutex. Randomness comes from the thread-local generator in [`crate::rng`].

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

use anyhow::{bail, Context, Result};

use crate::rng;
use crate::utils::fnvhash64;

/// `java.lang.Number` as produced by YCSB generators.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Number {
    Long(i64),
    Double(f64),
}

impl Number {
    /// `Number.intValue()`: wrapping for longs, saturating truncation for doubles.
    pub fn int_value(self) -> i32 {
        match self {
            Number::Long(v) => v as i32,
            Number::Double(v) => v as i32,
        }
    }

    /// `Number.longValue()`.
    pub fn long_value(self) -> i64 {
        match self {
            Number::Long(v) => v,
            Number::Double(v) => v as i64,
        }
    }
}

pub trait NumberGenerator: Send + Sync {
    fn next_value(&self) -> Number;
}

/// `ConstantIntegerGenerator`.
pub struct ConstantIntegerGenerator {
    value: i32,
}

impl ConstantIntegerGenerator {
    pub fn new(value: i32) -> Self {
        Self { value }
    }
}

impl NumberGenerator for ConstantIntegerGenerator {
    fn next_value(&self) -> Number {
        Number::Long(i64::from(self.value))
    }
}

/// `CounterGenerator`: hands out consecutive integers.
pub struct CounterGenerator {
    counter: AtomicI64,
}

impl CounterGenerator {
    pub fn new(start: i64) -> Self {
        Self {
            counter: AtomicI64::new(start),
        }
    }

    pub fn next(&self) -> i64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    pub fn last_value(&self) -> i64 {
        self.counter.load(Ordering::Relaxed) - 1
    }
}

impl NumberGenerator for CounterGenerator {
    fn next_value(&self) -> Number {
        Number::Long(self.next())
    }
}

const ACK_WINDOW_SIZE: usize = 1 << 20;
const ACK_WINDOW_MASK: i64 = (ACK_WINDOW_SIZE - 1) as i64;

/// `AcknowledgedCounterGenerator`: a counter whose `last_value` only advances past keys whose
/// insert has completed, so readers never pick a key that is still being written.
pub struct AcknowledgedCounterGenerator {
    counter: AtomicI64,
    window: Box<[AtomicBool]>,
    lock: Mutex<()>,
    limit: AtomicI64,
}

impl AcknowledgedCounterGenerator {
    pub fn new(start: i64) -> Self {
        Self {
            counter: AtomicI64::new(start),
            window: (0..ACK_WINDOW_SIZE).map(|_| AtomicBool::new(false)).collect(),
            lock: Mutex::new(()),
            limit: AtomicI64::new(start - 1),
        }
    }

    pub fn next(&self) -> i64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    pub fn last_value(&self) -> i64 {
        self.limit.load(Ordering::Acquire)
    }

    pub fn acknowledge(&self, value: i64) -> Result<()> {
        let slot = (value & ACK_WINDOW_MASK) as usize;
        if self.window[slot].swap(true, Ordering::AcqRel) {
            bail!("Too many unacknowledged insertion keys.");
        }
        if let Ok(_guard) = self.lock.try_lock() {
            let limit = self.limit.load(Ordering::Acquire);
            let before_first_slot = limit & ACK_WINDOW_MASK;
            let mut index = limit + 1;
            while index != before_first_slot {
                let slot = (index & ACK_WINDOW_MASK) as usize;
                if !self.window[slot].load(Ordering::Acquire) {
                    break;
                }
                self.window[slot].store(false, Ordering::Release);
                index += 1;
            }
            self.limit.store(index - 1, Ordering::Release);
        }
        Ok(())
    }
}

/// `UniformLongGenerator`: uniform over `[lb, ub]`.
pub struct UniformLongGenerator {
    lb: i64,
    interval: i64,
}

impl UniformLongGenerator {
    pub fn new(lb: i64, ub: i64) -> Self {
        Self {
            lb,
            interval: ub - lb + 1,
        }
    }

    pub fn next(&self) -> i64 {
        assert!(
            self.interval > 0,
            "UniformLongGenerator has an empty range (interval {})",
            self.interval
        );
        rng::next_i64().wrapping_abs() % self.interval + self.lb
    }
}

impl NumberGenerator for UniformLongGenerator {
    fn next_value(&self) -> Number {
        Number::Long(self.next())
    }
}

pub const ZIPFIAN_CONSTANT: f64 = 0.99;

/// `ZipfianGenerator` (Gray et al., "Quickly Generating Billion-Record Synthetic Databases").
pub struct ZipfianGenerator {
    items: i64,
    base: i64,
    theta: f64,
    zeta2theta: f64,
    alpha: f64,
    zetan_bits: AtomicU64,
    eta_bits: AtomicU64,
    count_for_zeta: AtomicI64,
    update_lock: Mutex<()>,
}

impl ZipfianGenerator {
    /// Items in `[0, items)`.
    pub fn with_items(items: i64) -> Self {
        Self::new(0, items - 1)
    }

    pub fn new(min: i64, max: i64) -> Self {
        Self::with_constant(min, max, ZIPFIAN_CONSTANT)
    }

    pub fn with_constant(min: i64, max: i64, constant: f64) -> Self {
        let zetan = zeta_static(0, max - min + 1, constant, 0.0);
        Self::with_zetan(min, max, constant, zetan)
    }

    pub fn with_zetan(min: i64, max: i64, constant: f64, zetan: f64) -> Self {
        let items = max - min + 1;
        let theta = constant;
        let zeta2theta = zeta_static(0, 2, theta, 0.0);
        let eta = eta(items, theta, zeta2theta, zetan);
        Self {
            items,
            base: min,
            theta,
            zeta2theta,
            alpha: 1.0 / (1.0 - theta),
            zetan_bits: AtomicU64::new(zetan.to_bits()),
            eta_bits: AtomicU64::new(eta.to_bits()),
            count_for_zeta: AtomicI64::new(items),
            update_lock: Mutex::new(()),
        }
    }

    /// Next value from a distribution over `item_count` items (which may grow over time).
    pub fn next_long(&self, item_count: i64) -> i64 {
        if item_count > self.count_for_zeta.load(Ordering::Acquire) {
            let _guard = self.update_lock.lock().expect("zipfian lock poisoned");
            let count_for_zeta = self.count_for_zeta.load(Ordering::Acquire);
            if item_count > count_for_zeta {
                // More items were added: extend zeta(n) incrementally.
                let zetan = f64::from_bits(self.zetan_bits.load(Ordering::Acquire));
                let zetan = zeta_static(count_for_zeta, item_count, self.theta, zetan);
                let eta = eta(self.items, self.theta, self.zeta2theta, zetan);
                self.zetan_bits.store(zetan.to_bits(), Ordering::Release);
                self.eta_bits.store(eta.to_bits(), Ordering::Release);
                self.count_for_zeta.store(item_count, Ordering::Release);
            }
        }
        let zetan = f64::from_bits(self.zetan_bits.load(Ordering::Acquire));
        let eta = f64::from_bits(self.eta_bits.load(Ordering::Acquire));
        let u = rng::next_f64();
        let uz = u * zetan;
        if uz < 1.0 {
            return self.base;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return self.base + 1;
        }
        self.base + (item_count as f64 * (eta * u - eta + 1.0).powf(self.alpha)) as i64
    }
}

impl NumberGenerator for ZipfianGenerator {
    fn next_value(&self) -> Number {
        Number::Long(self.next_long(self.items))
    }
}

fn eta(items: i64, theta: f64, zeta2theta: f64, zetan: f64) -> f64 {
    (1.0 - (2.0 / items as f64).powf(1.0 - theta)) / (1.0 - zeta2theta / zetan)
}

/// `ZipfianGenerator.zetastatic(st, n, theta, initialsum)`.
pub fn zeta_static(start: i64, n: i64, theta: f64, initial_sum: f64) -> f64 {
    let mut sum = initial_sum;
    let mut i = start;
    while i < n {
        sum += 1.0 / ((i + 1) as f64).powf(theta);
        i += 1;
    }
    sum
}

/// `ScrambledZipfianGenerator`: zipfian popularity, with popular items scattered across the
/// key space by hashing.
pub struct ScrambledZipfianGenerator {
    gen: ZipfianGenerator,
    min: i64,
    item_count: i64,
}

impl ScrambledZipfianGenerator {
    pub const ZETAN: f64 = 26.469_028_201_783_02;
    pub const ITEM_COUNT: i64 = 10_000_000_000;

    pub fn new(min: i64, max: i64) -> Self {
        Self {
            gen: ZipfianGenerator::with_zetan(0, Self::ITEM_COUNT, ZIPFIAN_CONSTANT, Self::ZETAN),
            min,
            item_count: max - min + 1,
        }
    }

    pub fn next(&self) -> i64 {
        let ret = self.gen.next_long(self.gen.items);
        self.min + fnvhash64(ret) % self.item_count
    }
}

impl NumberGenerator for ScrambledZipfianGenerator {
    fn next_value(&self) -> Number {
        Number::Long(self.next())
    }
}

/// `SkewedLatestGenerator`: zipfian skewed towards the most recently inserted keys.
pub struct SkewedLatestGenerator {
    basis: std::sync::Arc<AcknowledgedCounterGenerator>,
    zipfian: ZipfianGenerator,
}

impl SkewedLatestGenerator {
    pub fn new(basis: std::sync::Arc<AcknowledgedCounterGenerator>) -> Self {
        let zipfian = ZipfianGenerator::with_items(basis.last_value());
        Self { basis, zipfian }
    }
}

impl NumberGenerator for SkewedLatestGenerator {
    fn next_value(&self) -> Number {
        let max = self.basis.last_value();
        Number::Long(max - self.zipfian.next_long(max))
    }
}

/// `HotspotIntegerGenerator`: a hot fraction of the keys receives a hot fraction of the ops.
pub struct HotspotIntegerGenerator {
    lower_bound: i64,
    hot_interval: i64,
    cold_interval: i64,
    hot_opn_fraction: f64,
}

impl HotspotIntegerGenerator {
    pub fn new(
        mut lower_bound: i64,
        mut upper_bound: i64,
        mut hotset_fraction: f64,
        mut hot_opn_fraction: f64,
    ) -> Self {
        if !(0.0..=1.0).contains(&hotset_fraction) {
            eprintln!("Hotset fraction out of range. Setting to 0.0");
            hotset_fraction = 0.0;
        }
        if !(0.0..=1.0).contains(&hot_opn_fraction) {
            eprintln!("Hot operation fraction out of range. Setting to 0.0");
            hot_opn_fraction = 0.0;
        }
        if lower_bound > upper_bound {
            eprintln!("Upper bound of Hotspot generator smaller than the lower bound. Swapping the values.");
            std::mem::swap(&mut lower_bound, &mut upper_bound);
        }
        let interval = upper_bound - lower_bound + 1;
        let hot_interval = i64::from((interval as f64 * hotset_fraction) as i32);
        Self {
            lower_bound,
            hot_interval,
            cold_interval: interval - hot_interval,
            hot_opn_fraction,
        }
    }
}

impl NumberGenerator for HotspotIntegerGenerator {
    fn next_value(&self) -> Number {
        let value = if rng::next_f64() < self.hot_opn_fraction {
            self.lower_bound + rng::next_i64().wrapping_abs() % self.hot_interval
        } else {
            self.lower_bound + self.hot_interval + rng::next_i64().wrapping_abs() % self.cold_interval
        };
        Number::Long(value)
    }
}

/// `ExponentialGenerator`: most of the mass within `percentile`% of `range` from the end.
pub struct ExponentialGenerator {
    gamma: f64,
}

impl ExponentialGenerator {
    pub const PERCENTILE_PROPERTY: &'static str = "exponential.percentile";
    pub const PERCENTILE_DEFAULT: &'static str = "95";
    pub const FRAC_PROPERTY: &'static str = "exponential.frac";
    pub const FRAC_DEFAULT: &'static str = "0.8571428571";

    pub fn new(percentile: f64, range: f64) -> Self {
        Self {
            gamma: -(1.0 - percentile / 100.0).ln() / range,
        }
    }
}

impl NumberGenerator for ExponentialGenerator {
    fn next_value(&self) -> Number {
        Number::Double(-rng::next_f64().ln() / self.gamma)
    }
}

/// `SequentialGenerator`: cycles through `[start, end]` in order.
pub struct SequentialGenerator {
    counter: AtomicI64,
    interval: i64,
    start: i64,
}

impl SequentialGenerator {
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            counter: AtomicI64::new(0),
            interval: end - start + 1,
            start,
        }
    }
}

impl NumberGenerator for SequentialGenerator {
    fn next_value(&self) -> Number {
        Number::Long(self.start + self.counter.fetch_add(1, Ordering::Relaxed) % self.interval)
    }
}

/// `HistogramGenerator`: field lengths drawn from a `BlockSize`/bucket histogram file.
pub struct HistogramGenerator {
    block_size: i64,
    buckets: Vec<i64>,
    area: i64,
}

impl HistogramGenerator {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        Self::parse(&text)
    }

    pub fn parse(text: &str) -> Result<Self> {
        let mut lines = text.lines();
        let first = lines.next().context("Empty input file!")?;
        let mut header = first.split('\t');
        if header.next() != Some("BlockSize") {
            bail!("First line of histogram is not the BlockSize!");
        }
        let block_size: i64 = header.next().context("missing BlockSize value")?.trim().parse()?;
        let mut buckets: Vec<i64> = Vec::new();
        for line in lines {
            let mut parts = line.split('\t');
            let index: usize = parts.next().context("missing bucket")?.trim().parse()?;
            let value: i64 = parts.next().context("missing bucket value")?.trim().parse()?;
            if index > buckets.len() {
                bail!("histogram bucket {index} is out of order");
            }
            buckets.insert(index, value);
        }
        let area = buckets.iter().sum();
        Ok(Self {
            block_size,
            buckets,
            area,
        })
    }
}

impl NumberGenerator for HistogramGenerator {
    fn next_value(&self) -> Number {
        let mut number = rng::next_below(self.area as u64) as i64;
        let mut i = 0;
        while i + 1 < self.buckets.len() {
            number -= self.buckets[i];
            if number <= 0 {
                return Number::Long((i as i64 + 1) * self.block_size);
            }
            i += 1;
        }
        Number::Long(i as i64 * self.block_size)
    }
}

/// `DiscreteGenerator`: picks one of several labels by weight.
#[derive(Default)]
pub struct DiscreteGenerator {
    values: Vec<(f64, &'static str)>,
}

impl DiscreteGenerator {
    pub fn add_value(&mut self, weight: f64, value: &'static str) {
        self.values.push((weight, value));
    }

    pub fn next_string(&self) -> Option<&'static str> {
        let sum: f64 = self.values.iter().map(|(w, _)| w).sum();
        let mut val = rng::next_f64();
        for &(weight, value) in &self.values {
            let pw = weight / sum;
            if val < pw {
                return Some(value);
            }
            val -= pw;
        }
        // Floating point slack can leave a sliver past the last bucket; Java asserts here.
        self.values.last().map(|&(_, v)| v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn counter_generator_counts_from_start() {
        let c = CounterGenerator::new(5);
        assert_eq!(c.last_value(), 4);
        assert_eq!(c.next(), 5);
        assert_eq!(c.next(), 6);
        assert_eq!(c.last_value(), 6);
    }

    #[test]
    fn acknowledged_counter_advances_only_contiguously() {
        let c = AcknowledgedCounterGenerator::new(10);
        assert_eq!(c.last_value(), 9);
        let a = c.next();
        let b = c.next();
        let d = c.next();
        assert_eq!((a, b, d), (10, 11, 12));
        c.acknowledge(b).unwrap();
        assert_eq!(c.last_value(), 9, "11 acknowledged but 10 is still pending");
        c.acknowledge(a).unwrap();
        assert_eq!(c.last_value(), 11);
        c.acknowledge(d).unwrap();
        assert_eq!(c.last_value(), 12);
    }

    #[test]
    fn acknowledged_counter_rejects_double_acknowledge_of_pending_slot() {
        let c = AcknowledgedCounterGenerator::new(0);
        let _first = c.next();
        let second = c.next();
        c.acknowledge(second).unwrap();
        assert!(c.acknowledge(second).is_err());
    }

    #[test]
    fn acknowledged_counter_is_consistent_under_concurrency() {
        let c = Arc::new(AcknowledgedCounterGenerator::new(0));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let c = Arc::clone(&c);
                std::thread::spawn(move || {
                    for _ in 0..10_000 {
                        let v = c.next();
                        c.acknowledge(v).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // The final acknowledger may lose the try_lock race; one more ack settles the limit.
        let v = c.next();
        c.acknowledge(v).unwrap();
        assert_eq!(c.last_value(), 80_000);
    }

    #[test]
    fn uniform_generator_stays_in_bounds_and_covers_range() {
        let g = UniformLongGenerator::new(3, 7);
        let mut seen = HashMap::new();
        for _ in 0..10_000 {
            let v = g.next();
            assert!((3..=7).contains(&v));
            *seen.entry(v).or_insert(0) += 1;
        }
        assert_eq!(seen.len(), 5);
    }

    #[test]
    fn zipfian_generator_is_skewed_towards_low_items() {
        let g = ZipfianGenerator::new(0, 999);
        let mut counts = vec![0u32; 1000];
        for _ in 0..200_000 {
            let v = g.next_long(1000);
            assert!((0..1000).contains(&v), "{v}");
            counts[v as usize] += 1;
        }
        assert!(counts[0] > counts[10] && counts[10] > counts[500]);
        // With theta 0.99 over 1000 items, item 0 receives roughly 13% of draws.
        let share = f64::from(counts[0]) / 200_000.0;
        assert!((0.10..0.17).contains(&share), "{share}");
    }

    #[test]
    fn zipfian_generator_extends_zeta_when_items_grow() {
        let g = ZipfianGenerator::new(0, 99);
        for _ in 0..1000 {
            assert!(g.next_long(200) < 200);
        }
        let expected = zeta_static(0, 200, ZIPFIAN_CONSTANT, 0.0);
        let actual = f64::from_bits(g.zetan_bits.load(Ordering::Relaxed));
        assert!((expected - actual).abs() < 1e-9);
    }

    #[test]
    fn scrambled_zipfian_constant_matches_java() {
        // Java hardcodes zeta(10^10, 0.99); spot-check the formula on a smaller prefix instead
        // of the full 10^10-term sum.
        assert!((zeta_static(0, 2, 0.99, 0.0) - (1.0 + 1.0 / 2f64.powf(0.99))).abs() < 1e-12);
        let g = ScrambledZipfianGenerator::new(100, 199);
        for _ in 0..10_000 {
            assert!((100..=199).contains(&g.next()));
        }
    }

    #[test]
    fn skewed_latest_prefers_recent_keys() {
        let basis = Arc::new(AcknowledgedCounterGenerator::new(1000));
        let g = SkewedLatestGenerator::new(Arc::clone(&basis));
        let mut recent = 0;
        for _ in 0..10_000 {
            let v = g.next_value().long_value();
            assert!((0..=999).contains(&v), "{v}");
            if v >= 990 {
                recent += 1;
            }
        }
        // The newest 10 of 999 keys get zeta(10) / zeta(999) of the draws (~38%).
        let expected = zeta_static(0, 10, ZIPFIAN_CONSTANT, 0.0) / zeta_static(0, 999, ZIPFIAN_CONSTANT, 0.0);
        let share = f64::from(recent) / 10_000.0;
        assert!((share - expected).abs() < 0.04, "share {share}, expected {expected}");
    }

    #[test]
    fn hotspot_generator_sends_most_ops_to_hot_set() {
        let g = HotspotIntegerGenerator::new(0, 999, 0.2, 0.8);
        let hot = (0..20_000).filter(|_| g.next_value().long_value() < 200).count();
        let share = hot as f64 / 20_000.0;
        assert!((0.77..0.83).contains(&share), "{share}");
    }

    #[test]
    fn exponential_generator_mass_is_mostly_within_range() {
        let g = ExponentialGenerator::new(95.0, 1000.0);
        let within = (0..20_000).filter(|_| g.next_value().int_value() < 1000).count();
        let share = within as f64 / 20_000.0;
        assert!((0.93..0.97).contains(&share), "{share}");
    }

    #[test]
    fn sequential_generator_wraps() {
        let g = SequentialGenerator::new(5, 7);
        let v: Vec<i64> = (0..5).map(|_| g.next_value().long_value()).collect();
        assert_eq!(v, vec![5, 6, 7, 5, 6]);
    }

    #[test]
    fn histogram_generator_follows_java_sampling() {
        // Java draws `number` in [0, area) and returns (i + 1) * blockSize at the first bucket
        // where the running remainder drops to <= 0, falling back to (len - 1) * blockSize.
        // With buckets [0, 5, 5] that yields 10 for a draw of 0 and 20 otherwise.
        let g = HistogramGenerator::parse("BlockSize\t10\n0\t0\n1\t5\n2\t5\n").unwrap();
        let mut tens = 0;
        for _ in 0..10_000 {
            match g.next_value().long_value() {
                10 => tens += 1,
                20 => {}
                other => panic!("unexpected sample {other}"),
            }
        }
        assert!((800..1200).contains(&tens), "{tens}");
        assert!(HistogramGenerator::parse("nope\t1\n").is_err());
    }

    #[test]
    fn discrete_generator_respects_weights() {
        let mut g = DiscreteGenerator::default();
        g.add_value(0.95, "READ");
        g.add_value(0.05, "UPDATE");
        let reads = (0..20_000).filter(|_| g.next_string() == Some("READ")).count();
        let share = reads as f64 / 20_000.0;
        assert!((0.94..0.96).contains(&share), "{share}");
        assert_eq!(DiscreteGenerator::default().next_string(), None);
    }

    #[test]
    fn number_conversions_follow_java() {
        assert_eq!(Number::Long(1 << 33).int_value(), 0);
        assert_eq!(Number::Double(3.9).int_value(), 3);
        assert_eq!(Number::Double(f64::INFINITY).int_value(), i32::MAX);
        assert_eq!(Number::Double(f64::NAN).int_value(), 0);
    }
}
