//! Thread-local pseudo random numbers, standing in for Java's `ThreadLocalRandom`.
//!
//! YCSB only needs fast, statistically reasonable randomness (key choice, operation mix,
//! field values), not cryptographic randomness. Each thread gets an independent wyrand
//! generator seeded from the OS-randomized `RandomState` plus a per-thread counter.

use std::cell::Cell;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

static THREAD_SEQ: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static STATE: Cell<u64> = Cell::new(initial_seed());
}

fn initial_seed() -> u64 {
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(THREAD_SEQ.fetch_add(1, Ordering::Relaxed));
    hasher.finish()
}

/// Next 64 random bits (wyrand).
#[inline]
pub fn next_u64() -> u64 {
    STATE.with(|state| {
        let s = state.get().wrapping_add(0xa076_1d64_78bd_642f);
        state.set(s);
        let t = u128::from(s) * u128::from(s ^ 0xe703_7ed1_a0b4_28db);
        ((t >> 64) as u64) ^ (t as u64)
    })
}

/// Equivalent of `Random.nextLong()`.
#[inline]
pub fn next_i64() -> i64 {
    next_u64() as i64
}

/// Equivalent of `Random.nextInt()`.
#[inline]
pub fn next_i32() -> i32 {
    (next_u64() >> 32) as i32
}

/// Uniform double in `[0, 1)`, equivalent of `Random.nextDouble()`.
#[inline]
pub fn next_f64() -> f64 {
    (next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
}

/// Uniform integer in `[0, bound)`. Returns 0 when `bound` is 0.
#[inline]
pub fn next_below(bound: u64) -> u64 {
    if bound == 0 {
        return 0;
    }
    ((u128::from(next_u64()) * u128::from(bound)) >> 64) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_f64_is_in_unit_interval() {
        for _ in 0..100_000 {
            let v = next_f64();
            assert!((0.0..1.0).contains(&v), "{v}");
        }
    }

    #[test]
    fn next_below_respects_bound() {
        for bound in [1u64, 2, 3, 7, 1000] {
            for _ in 0..10_000 {
                assert!(next_below(bound) < bound);
            }
        }
        assert_eq!(next_below(0), 0);
    }

    #[test]
    fn next_below_is_roughly_uniform() {
        let mut buckets = [0u32; 10];
        let n = 200_000;
        for _ in 0..n {
            buckets[next_below(10) as usize] += 1;
        }
        for count in buckets {
            let expected = n as f64 / 10.0;
            assert!((f64::from(count) - expected).abs() < expected * 0.05, "{buckets:?}");
        }
    }

    #[test]
    fn threads_get_different_streams() {
        let a: Vec<u64> = std::thread::spawn(|| (0..4).map(|_| next_u64()).collect())
            .join()
            .unwrap();
        let b: Vec<u64> = std::thread::spawn(|| (0..4).map(|_| next_u64()).collect())
            .join()
            .unwrap();
        assert_ne!(a, b);
    }
}
