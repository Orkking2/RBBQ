# rbbq

A bounded multi-producer, multi-consumer FIFO queue in Rust, based on BBQ, the
block-based bounded queue:

> Jiawei Wang, Diogo Behrens, Ming Fu, Lilith Oberhauser, Jonas Oberhauser,
> Jitang Lei, Geng Chen, Hermann Härtig, and Haibo Chen. *BBQ: A Block-based
> Bounded Queue for Exchanging Data and Profiling.* USENIX ATC 2022.
> [Paper page](https://www.usenix.org/conference/atc22/presentation/wang-jiawei)

The ring is divided into blocks. Threads write to the shared heads only when
they move between blocks, and within a block they work on per-block cursors.
The queue implements the paper's retry-new mode: when the queue is full,
producers get an error instead of overwriting old data.

## Design

- **Versioned indices in one word.** Each cursor packs a version and an index
  into a single `usize` (`src/field.rs`), so an index and its version are read
  and advanced atomically, and a monotonic `fetch_max` can safely move a cursor
  forward when several threads race to do so.
- **Four cursors per block.** `allocated` and `committed` track producers;
  `reserved` and `consumed` track consumers (`src/block.rs`). Claiming a slot
  and finishing work on it are separate steps, so a consumer can tell a slot
  that is still being written from one that does not exist yet.
- **Padding against false sharing.** Each per-block cursor sits in its own
  128-byte-aligned wrapper (`Line128` in `src/atom_pair.rs`), so producers and
  consumers in the same block do not share a cache line.
- **Deliberate memory orderings.** Publishing an entry is a `Release` increment
  of `committed` or `consumed`, paired with an `Acquire` load on the other side.
  Claiming a slot and advancing a head use `Relaxed` operations.
- **Block-level reclamation.** Producers can reuse a block only after every
  entry in it has been consumed, so `Full` can be returned while part of a block
  is free. Capacity is `num_blocks * block_size`.

## API

| Method | Purpose |
| --- | --- |
| `FastFifo::new(num_blocks, block_size)` | Create a queue. Requires at least two blocks. Clones share the queue. |
| `push`, `pop` | Move a value in or out. |
| `push_in_place`, `pop_in_place` | Run a closure directly on the slot's `*mut T`. |
| `try_get_producer_entry`, `try_get_consumer_entry` | Hold a slot as a guard. The slot is published to the other side when the guard drops. |
| `indexed_push`, `indexed_pop` | Push to a given slot, or pop and learn which slot the value came from. |

Fallible operations return an error rather than blocking:
`Full` or `Empty` when there is no room or no data, and `Busy` when another
thread's outstanding entry is in the way. Callers choose how to retry.

## Usage

```rust
use rbbq::FastFifo;
use std::thread;

// 4 blocks of 16 entries each: room for 64 values.
let fifo = FastFifo::new(4, 16);

let producer = {
    let fifo = fifo.clone();
    thread::spawn(move || {
        for i in 0..1_000u64 {
            while fifo.push(i).is_err() {
                thread::yield_now();
            }
        }
    })
};

let mut expected = 0;
while expected < 1_000u64 {
    match fifo.pop() {
        Ok(value) => {
            assert_eq!(value, expected);
            expected += 1;
        }
        Err(_) => thread::yield_now(),
    }
}
producer.join().unwrap();

// Zero-copy: write into, and read from, the slot itself.
fifo.push_in_place(|slot| unsafe { slot.write(7) }).unwrap();
let mut out = 0;
fifo.pop_in_place(|slot| out = unsafe { slot.read() }).unwrap();
assert_eq!(out, 7);
```

The in-place closures receive a raw pointer. A producer closure must initialize
the slot, and a consumer closure must take ownership of the value or drop it.

## Testing

```sh
cargo test
```

The suite covers FIFO order across block wraparounds, full, empty and busy
behavior, each entry API, and a multi-producer, multi-consumer test that checks
exactly-once delivery and per-producer order. Tests marked `#[ignore]` document
known bugs. Run them with `cargo test -- --ignored`.

`src/atom_pair.rs` contains `cfg(loom)` hooks for loom model checking.

## License

MIT. See [LICENSE](LICENSE).
