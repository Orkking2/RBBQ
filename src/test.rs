use crate::FastFifo;
use crate::error::Error;
use crate::fifo_inner::FifoIndex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn constructs_with_various_shapes() {
    for (num_blocks, block_size) in [(2, 1), (2, 2), (3, 7), (10, 10), (16, 256)] {
        let fifo = FastFifo::<u64>::new(num_blocks, block_size);
        assert_eq!(fifo.pop(), Err(Error::Empty));
    }
}

#[test]
#[should_panic(expected = "only one block")]
fn rejects_a_single_block() {
    let _ = FastFifo::<u64>::new(1, 10);
}

/// Batches of up to `(num_blocks - 1) * block_size` always fit, wherever in a
/// block the producer head happens to be, so each batch can be pushed whole.
#[test]
fn preserves_fifo_order_across_many_wraparounds() {
    for (num_blocks, block_size) in [(2, 1), (2, 4), (3, 7), (4, 10), (16, 64)] {
        let fifo = FastFifo::new(num_blocks, block_size);
        let max_batch = (num_blocks - 1) * block_size;
        let mut next_push = 0u64;
        let mut next_pop = 0u64;

        for batch in [1, block_size, block_size + 1, max_batch] {
            let batch = batch.min(max_batch);
            // Enough rounds for this batch size to lap the ring 50 times.
            let rounds = 50 * num_blocks * block_size / batch;
            for _ in 0..rounds {
                for _ in 0..batch {
                    fifo.push(next_push).unwrap();
                    next_push += 1;
                }
                for _ in 0..batch {
                    assert_eq!(fifo.pop(), Ok(next_pop));
                    next_pop += 1;
                }
                assert_eq!(fifo.pop(), Err(Error::Empty));
            }
        }
    }
}

#[test]
fn reports_empty_and_full() {
    let (num_blocks, block_size) = (4, 8);
    let capacity = (num_blocks * block_size) as u64;
    let fifo = FastFifo::new(num_blocks, block_size);

    // The queue must be reusable after being filled and drained.
    for lap in 0..3 {
        assert_eq!(fifo.pop(), Err(Error::Empty));
        for i in 0..capacity {
            fifo.push(lap * capacity + i).unwrap();
        }
        assert_eq!(fifo.push(u64::MAX), Err(Error::Full));
        for i in 0..capacity {
            assert_eq!(fifo.pop(), Ok(lap * capacity + i));
        }
        assert_eq!(fifo.pop(), Err(Error::Empty));
    }
}

/// Reclamation is per block: producers cannot reuse a block until every entry
/// in it has been consumed.
#[test]
fn reclaims_space_one_block_at_a_time() {
    let fifo = FastFifo::new(2, 4);
    for i in 0..8u64 {
        fifo.push(i).unwrap();
    }
    for i in 0..3u64 {
        assert_eq!(fifo.pop(), Ok(i));
        assert_eq!(fifo.push(100), Err(Error::Full));
    }
    assert_eq!(fifo.pop(), Ok(3));
    fifo.push(100).unwrap();
}

/// A consumer must not overtake a producer that has allocated a slot but has
/// not committed it yet.
#[test]
fn consumers_see_busy_while_a_producer_entry_is_outstanding() {
    let fifo = FastFifo::new(2, 4);
    fifo.push(1u64).unwrap();
    let mut entry = fifo.try_get_producer_entry().unwrap();

    assert_eq!(fifo.pop(), Err(Error::Busy));

    entry.produce_t_in_place(|slot| unsafe { slot.write(2) });
    drop(entry);
    assert_eq!(fifo.pop(), Ok(1));
    assert_eq!(fifo.pop(), Ok(2));
}

/// A producer must not reclaim a block in which a consumer entry is still
/// outstanding.
#[test]
fn producers_see_busy_while_a_consumer_entry_is_outstanding() {
    let fifo = FastFifo::new(2, 2);
    for i in 0..4u64 {
        fifo.push(i).unwrap();
    }
    assert_eq!(fifo.pop(), Ok(0));
    let mut entry = fifo.try_get_consumer_entry().unwrap();

    assert_eq!(fifo.push(4), Err(Error::Busy));

    let mut seen = None;
    entry.consume_t_in_place(|slot| seen = Some(unsafe { slot.read() }));
    drop(entry);
    assert_eq!(seen, Some(1));
    fifo.push(4).unwrap();
}

#[test]
fn in_place_closures_round_trip() {
    let fifo = FastFifo::<u64>::new(3, 4);
    for i in 0..100u64 {
        fifo.push_in_place(|slot| unsafe { slot.write(i * 2) })
            .unwrap();
        let mut out = None;
        fifo.pop_in_place(|slot| out = Some(unsafe { slot.read() }))
            .unwrap();
        assert_eq!(out, Some(i * 2));
    }
    assert!(matches!(
        fifo.pop_in_place(|_| panic!("closure ran on an empty queue")),
        Err(Error::Empty)
    ));
}

/// Entries hand out the slot itself, so an owning type moves in and out
/// without an intermediate copy on either side of the queue.
#[test]
fn entries_round_trip_owned_values() {
    let fifo = FastFifo::<String>::new(2, 4);
    for i in 0..32 {
        let mut producer = fifo.try_get_producer_entry().unwrap();
        producer.produce_t_in_place(|slot| unsafe { slot.write(format!("value {i}")) });
        drop(producer);

        let mut consumer = fifo.try_get_consumer_entry().unwrap();
        let mut out = None;
        consumer.consume_t_in_place(|slot| out = Some(unsafe { slot.read() }));
        drop(consumer);
        assert_eq!(out.as_deref(), Some(format!("value {i}").as_str()));
    }
}

#[test]
fn indexed_push_and_pop_address_a_slot() {
    let fifo = FastFifo::new(2, 4);
    for sub_block_idx in 0..3 {
        fifo.indexed_push(
            sub_block_idx as u64 + 10,
            FifoIndex {
                block_idx: 0,
                sub_block_idx,
            },
        );
    }
    for sub_block_idx in 0..3 {
        let (value, index) = fifo.indexed_pop().unwrap();
        assert_eq!(value, sub_block_idx as u64 + 10);
        assert_eq!(index.block_idx, 0);
        assert_eq!(index.sub_block_idx, sub_block_idx);
    }
    assert!(matches!(fifo.indexed_pop(), Err(Error::Empty)));
}

#[test]
fn indexed_pop_reports_the_block_it_read_from() {
    let fifo = FastFifo::new(2, 2);
    for i in 0..3u64 {
        fifo.push(i).unwrap();
    }
    let _ = fifo.indexed_pop().unwrap();
    let _ = fifo.indexed_pop().unwrap();
    let (value, index) = fifo.indexed_pop().unwrap();
    assert_eq!(value, 2);
    assert_eq!(index.sub_block_idx, 0);
    assert_eq!(index.block_idx, 1);
}

#[derive(Debug)]
struct CountsDrops(Arc<AtomicUsize>);

impl Drop for CountsDrops {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn drops_values_left_in_the_first_block() {
    let drops = Arc::new(AtomicUsize::new(0));
    let fifo = FastFifo::new(4, 8);
    for _ in 0..6 {
        fifo.push(CountsDrops(drops.clone())).unwrap();
    }
    drop(fifo.pop().ok());
    drop(fifo.pop().ok());
    assert_eq!(drops.load(Ordering::Relaxed), 2);

    drop(fifo);
    assert_eq!(drops.load(Ordering::Relaxed), 6);
}

#[test]
fn drops_values_in_blocks_consumers_have_not_reached() {
    let drops = Arc::new(AtomicUsize::new(0));
    let fifo = FastFifo::new(4, 8);
    for _ in 0..20 {
        fifo.push(CountsDrops(drops.clone())).unwrap();
    }

    drop(fifo);
    assert_eq!(drops.load(Ordering::Relaxed), 20);
}

/// After a full lap the first block is reused, so its `reserved` cursor has a
/// previous version until consumers arrive, and then the current one.
#[test]
fn drops_exactly_the_live_values_after_a_lap() {
    let drops = Arc::new(AtomicUsize::new(0));
    let fifo = FastFifo::new(2, 4);
    for _ in 0..8 {
        fifo.push(CountsDrops(drops.clone())).unwrap();
        drop(fifo.pop().ok());
    }
    assert_eq!(drops.load(Ordering::Relaxed), 8);

    for _ in 0..3 {
        fifo.push(CountsDrops(drops.clone())).unwrap();
    }
    drop(fifo.pop().ok());
    assert_eq!(drops.load(Ordering::Relaxed), 9);

    drop(fifo);
    assert_eq!(drops.load(Ordering::Relaxed), 11);
}

#[test]
fn debug_only_inspects_owned_values() {
    let fifo = FastFifo::new(2, 4);
    fifo.push("first".to_string()).unwrap();
    fifo.push("second".to_string()).unwrap();

    let printed = format!("{fifo:?}");
    assert!(printed.contains("first") && printed.contains("second"));

    assert_eq!(fifo.pop().unwrap(), "first");
    assert_eq!(fifo.pop().unwrap(), "second");
}

#[test]
fn debug_does_not_drop_values() {
    let drops = Arc::new(AtomicUsize::new(0));
    let fifo = FastFifo::new(2, 4);
    fifo.push(CountsDrops(drops.clone())).unwrap();

    let _ = format!("{fifo:?}");
    assert_eq!(drops.load(Ordering::Relaxed), 0);

    drop(fifo);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn is_send_and_sync_for_send_values() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<FastFifo<String>>();
}

fn tag(producer: usize, seq: usize) -> u64 {
    ((producer as u64) << 32) | seq as u64
}

fn untag(value: u64) -> (usize, usize) {
    ((value >> 32) as usize, (value & 0xffff_ffff) as usize)
}

/// Checks that every pushed value is popped exactly once, and that each
/// consumer observes each producer's values in the order they were pushed.
fn check_mpmc(
    producer_count: usize,
    consumer_count: usize,
    items_per_producer: usize,
    num_blocks: usize,
    block_size: usize,
) {
    let total = producer_count * items_per_producer;

    // A lost value must fail the test rather than spin forever.
    let deadline = Instant::now() + Duration::from_secs(30);
    let fifo = FastFifo::<u64>::new(num_blocks, block_size);
    let popped = Arc::new(AtomicUsize::new(0));

    let producers: Vec<_> = (0..producer_count)
        .map(|producer| {
            let fifo = fifo.clone();
            thread::spawn(move || {
                for seq in 0..items_per_producer {
                    while fifo.push(tag(producer, seq)).is_err() {
                        assert!(Instant::now() < deadline, "producer timed out");
                        thread::yield_now();
                    }
                }
            })
        })
        .collect();

    let consumers: Vec<_> = (0..consumer_count)
        .map(|_| {
            let fifo = fifo.clone();
            let popped = popped.clone();
            thread::spawn(move || {
                let mut received = Vec::new();
                while popped.load(Ordering::Relaxed) < total && Instant::now() < deadline {
                    match fifo.pop() {
                        Ok(value) => {
                            received.push(value);
                            popped.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => thread::yield_now(),
                    }
                }
                received
            })
        })
        .collect();

    for producer in producers {
        producer.join().unwrap();
    }
    let received: Vec<Vec<u64>> = consumers
        .into_iter()
        .map(|consumer| consumer.join().unwrap())
        .collect();

    let mut delivery_counts = vec![0u8; total];
    for log in &received {
        let mut last_seq = vec![None; producer_count];
        for &value in log {
            let (producer, seq) = untag(value);
            assert!(
                producer < producer_count && seq < items_per_producer,
                "value {value:#x} was never pushed"
            );
            assert!(
                last_seq[producer].is_none_or(|last| last < seq),
                "producer {producer}: saw seq {seq} after {:?}",
                last_seq[producer]
            );
            last_seq[producer] = Some(seq);

            let slot = &mut delivery_counts[producer * items_per_producer + seq];
            *slot = slot.saturating_add(1);
        }
    }

    let missing = delivery_counts.iter().filter(|&&n| n == 0).count();
    let duplicated = delivery_counts.iter().filter(|&&n| n > 1).count();
    assert_eq!(
        (missing, duplicated),
        (0, 0),
        "(missing, duplicated) values"
    );
    assert_eq!(fifo.pop(), Err(Error::Empty));
}

/// The queue is much smaller than the workload, so blocks are recycled
/// constantly under contention.
#[test]
fn mpmc_delivers_each_value_exactly_once_in_producer_order() {
    check_mpmc(4, 4, 50_000, 8, 64);
}

/// With these block sizes the offset of a full block has no spare values below
/// the next power of two, so a producer that overshoots the end of the block
/// must not carry into the version bits.
#[test]
fn mpmc_survives_producers_overshooting_a_block_of_two_to_the_k_minus_one() {
    for block_size in [3, 7, 15] {
        for _ in 0..10 {
            check_mpmc(8, 4, 2_000, 4, block_size);
        }
    }
}
