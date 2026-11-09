//! Exact-once stress test: every pushed value must be popped exactly once.
//!
//! The queue is reused across rounds, as a benchmark harness does, so that
//! blocks are recycled many times. Values are `0..items` in every round and a
//! per-round bitmap records each delivery, so a value handed to two consumers,
//! or one read from a stale slot, is counted as a duplicate, and a value never
//! seen is counted as missing.
//!
//! Scale with the environment for long runs, e.g.
//! `RBBQ_STRESS_ROUNDS=2000 cargo test --release --test exact_once -- --nocapture`.

use rbbq::FastFifo;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::thread;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Outcome {
    duplicates: u64,
    missing: u64,
    consumed: u64,
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Exponential backoff in the manner of crossbeam's `Backoff`, which the lab's
/// adapter uses: spin 1, 2, 4, ... 64 times, then yield. Waiters stay hot, so
/// many consumers poll the queue at once when it runs dry.
struct Backoff(u32);

impl Backoff {
    fn spin(&mut self) {
        for _ in 0..1u32 << self.0.min(6) {
            std::hint::spin_loop();
        }
        self.0 = (self.0 + 1).min(6);
    }

    fn snooze(&mut self) {
        if self.0 <= 6 {
            for _ in 0..1u32 << self.0 {
                std::hint::spin_loop();
            }
        } else {
            thread::yield_now();
        }
        self.0 = (self.0 + 1).min(10);
    }
}

/// Push with retry, as the lab's adapter does: `Full` and `Busy` are retried.
fn push(fifo: &FastFifo<u64>, value: u64) {
    let mut backoff = Backoff(0);
    while fifo.push(value).is_err() {
        backoff.snooze();
    }
}

/// Pop as the lab's adapter does: eight tries, then report nothing.
fn pop(fifo: &FastFifo<u64>) -> Option<u64> {
    let mut backoff = Backoff(0);
    for _ in 0..8 {
        if let Ok(v) = fifo.pop() {
            return Some(v);
        }
        backoff.spin();
    }
    None
}

fn run(
    producers: usize,
    consumers: usize,
    num_blocks: usize,
    block_size: usize,
    items: usize,
    rounds: usize,
) -> Outcome {
    let fifo = FastFifo::<u64>::new(num_blocks, block_size);
    let seen: Vec<AtomicU8> = (0..items).map(|_| AtomicU8::new(0)).collect();
    let mut total = Outcome::default();
    let mut reported = 0;
    for round in 0..rounds {
        for s in &seen {
            s.store(0, Ordering::Relaxed);
        }
        let done = AtomicBool::new(false);
        let duplicates = AtomicU64::new(0);
        let consumed = AtomicU64::new(0);
        thread::scope(|scope| {
            let mut handles = vec![];
            for p in 0..producers {
                let fifo = &fifo;
                handles.push(scope.spawn(move || {
                    let mut v = p;
                    while v < items {
                        push(fifo, v as u64);
                        v += producers;
                    }
                }));
            }
            for _ in 0..consumers {
                let (fifo, seen, done) = (&fifo, &seen, &done);
                let (duplicates, consumed) = (&duplicates, &consumed);
                scope.spawn(move || {
                    let mut after_done_empty = false;
                    let (mut dup, mut got) = (0, 0);
                    let mut backoff = Backoff(0);
                    loop {
                        if let Some(v) = pop(fifo) {
                            got += 1;
                            after_done_empty = false;
                            if seen[v as usize].swap(1, Ordering::Relaxed) != 0 {
                                dup += 1;
                            }
                            continue;
                        }
                        if done.load(Ordering::Acquire) {
                            if after_done_empty {
                                break;
                            }
                            after_done_empty = true;
                        }
                        backoff.snooze();
                    }
                    duplicates.fetch_add(dup, Ordering::Relaxed);
                    consumed.fetch_add(got, Ordering::Relaxed);
                });
            }
            for h in handles {
                h.join().unwrap();
            }
            done.store(true, Ordering::Release);
        });
        let (dup, got) = (duplicates.load(Ordering::Relaxed), consumed.load(Ordering::Relaxed));
        let missing = seen.iter().filter(|s| s.load(Ordering::Relaxed) == 0).count() as u64;
        if (dup, missing) != (0, 0) && reported < 8 {
            reported += 1;
            eprintln!(
                "  round {round}: consumed {got} of {items}, duplicates {dup}, missing {missing}, \
                 producer offset in block at round end {}",
                ((round + 1) * items) % block_size
            );
        }
        total.duplicates += dup;
        total.consumed += got;
        total.missing += missing;
    }
    total
}

fn check(producers: usize, consumers: usize, num_blocks: usize, block_size: usize) {
    // A block's `allocated` offset may overshoot `block_size` by up to one per
    // producer; its bit field holds offsets below the next power of two above
    // `block_size`, so keep the producers within that headroom.
    assert!(producers <= (block_size + 1).next_power_of_two() - block_size);
    let rounds = env_usize("RBBQ_STRESS_ROUNDS", 40);
    let items = env_usize("RBBQ_STRESS_ITEMS", 1 << 14);
    let out = run(producers, consumers, num_blocks, block_size, items, rounds);
    eprintln!(
        "{producers}p{consumers}c blocks={num_blocks} block_size={block_size} rounds={rounds} \
         items={items}: {out:?}"
    );
    assert_eq!(
        (out.duplicates, out.missing),
        (0, 0),
        "{producers}p{consumers}c blocks={num_blocks} block_size={block_size}: {out:?}"
    );
    assert_eq!(out.consumed, (items * rounds) as u64);
}

fn threads() -> usize {
    env_usize(
        "RBBQ_STRESS_THREADS",
        thread::available_parallelism().map_or(4, |n| n.get()),
    )
    .max(2)
}

#[test]
fn one_producer_many_consumers() {
    let c = threads() - 1;
    for (blocks, size) in [(2, 4), (4, 8), (8, 64), (4, 256)] {
        check(1, c, blocks, size);
    }
}

#[test]
fn many_producers_many_consumers() {
    let half = threads() / 2;
    for (blocks, size) in [(2, 16), (4, 32), (8, 64), (4, 256)] {
        check(half, half, blocks, size);
    }
}

#[test]
fn many_producers_one_consumer() {
    let p = threads() - 1;
    for (blocks, size) in [(2, 16), (4, 32), (8, 64), (4, 256)] {
        check(p, 1, blocks, size);
    }
}

/// One shape and geometry from the environment, for long or oversubscribed
/// runs: `RBBQ_STRESS_SHAPE=1p159c RBBQ_STRESS_BLOCKS=4099
/// RBBQ_STRESS_BLOCK_SIZE=256 cargo test --release --test exact_once custom -- --ignored`.
#[test]
#[ignore]
fn custom_shape() {
    let shape = std::env::var("RBBQ_STRESS_SHAPE").unwrap_or_else(|_| "1p64c".into());
    let (p, c) = shape
        .trim_end_matches('c')
        .split_once('p')
        .map(|(p, c)| (p.parse().unwrap(), c.parse().unwrap()))
        .expect("shape like 4p4c");
    check(
        p,
        c,
        env_usize("RBBQ_STRESS_BLOCKS", 8),
        env_usize("RBBQ_STRESS_BLOCK_SIZE", 64),
    );
}
