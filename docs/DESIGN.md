# riverlog design notes

## Storage

A topic is a directory of partitions. A partition is a directory of segments:

```text
data/topics/<topic>/<partition>/00000000000000000000.log   record batches
                                00000000000000000000.idx   sparse index (rel offset, byte position)
                                00000000000000183942.log   next segment, named by its base offset
```

A segment is a concatenation of record batches. The batch format is shared
by disk and wire, so a fetch returns the stored bytes as they are:

```text
baseOffset u64 | length u32 | crc32c u32 | count u32 | firstTs i64 | maxTs i64 | records...
record: keyLen varint (-1 = nil) | key | valueLen uvarint | value
```

The CRC covers everything after the CRC field, so the broker can assign
`baseOffset` without recomputing it. Producers build batches, and the broker
validates each one (length, CRC, and that the records parse to the declared
count) before appending.

The index stores one entry every 4 KiB of log (configurable). A read
binary-searches for the segment by base offset, then for the index entry,
and scans batch headers from there. Index files are derived data: they are
rebuilt from the log whenever a segment is opened.

## Recovery

On startup each partition opens all its segments. Every segment is scanned
batch by batch, checking length, CRC, record parse, and that each batch's
base offset continues the previous one. For the active (last) segment, the
first failure point is the truncation point. Whatever follows was a torn
write. For an older segment, a failure is reported as corruption, because
those segments were fsynced before the next segment was created.

`TestRecoveryFromTornWrites` truncates, appends garbage to, or flips a bit
in the active segment at random positions. It checks that exactly the
complete batches before the damage survive, and that appends then continue
at the right offset.

## Durability and group commit

| `-sync` | producer ack | readers see |
|---|---|---|
| `always` | after fsync | only fsynced data |
| `interval` | after write | everything written (fsync every N ms) |
| `never` | after write | everything written |

Under `always`, appends are split in two. `AppendNoWait` writes the batch
under the partition lock and returns its end offset. `WaitDurable(end)` then
waits until that offset is fsynced. Any waiter that finds no fsync in
progress starts one for everything written so far, with the lock released.
Waiters that arrive meanwhile are covered by the next one. So N concurrent
producers cost about one fsync per round, not N.

The broker does the append inline in the connection's read loop, which keeps
a connection's batches in order, and does the wait in a goroutine. A client
that pipelines batches on one connection therefore also shares fsyncs.

Readers see only durable data under `always`. Otherwise a consumer could read
a record that a crash then erased, and the producer was never told it
succeeded.

On macOS, Go's `File.Sync` issues `F_FULLFSYNC`, which also flushes the
drive's write cache. Java's `FileChannel.force`, which Kafka uses, has done
the same since JDK 9 (JDK-8080589). `-fsync plain` uses `fsync(2)`, which
survives a process crash but not necessarily a power loss. The benchmarks
compare all three systems with `F_FULLFSYNC`, and report plain `fsync` as an
extra riverlog row.

## Protocol

Frames are length-prefixed. Requests carry an API id and a correlation id,
and responses echo the correlation id, so a client can pipeline requests
and match responses as they arrive. Produce responses wait for the fsync,
and fetch responses wait for data. A long-poll fetch therefore never blocks
a produce on the same connection.

## Consumer groups

The broker coordinates groups and computes assignments itself, with
range assignment per topic over sorted member ids. There is no group leader.

1. A join from a new member, a member leaving, or a missed session deadline
   starts a rebalance.
2. Heartbeats from the old generation get `RebalanceInProgress`, which
   sends members back to `JoinGroup`.
3. `JoinGroup` blocks until every member has re-joined. A rebalance timeout
   drops stragglers. Then all waiters get the new generation and their
   assignments together.
4. Offset commits are accepted only from a member of the current
   generation, for a partition assigned to it. A consumer that missed the
   rebalance is fenced.

Committed offsets are stored per group in a JSON file: written to a temp
file, fsynced, renamed, and then the directory is fsynced.

Delivery is at-least-once. Records after the last commit are redelivered to
the partition's next owner. `TestConsumerGroupRebalance` runs four consumers
joining and leaving while 30,000 records flow. It checks that every record
is consumed and that no partition has two owners within a generation.

## Crash test

`TestCrashKeepsAcknowledgedRecords` starts the real binary with
`-sync always` and runs four pipelining producers. It `SIGKILL`s the broker
at random moments over 8 rounds and restarts it each time. It then checks
that every acknowledged record is present at exactly its acknowledged
offset, that offsets have no gaps, and that there are no duplicates. It runs
under both `-fsync full` and `-fsync plain`.

This tests process crashes. The page cache survives them, so it does not
test power loss. Surviving power loss depends on `-fsync full` and the drive
honouring flushes.

## Not done

- Replication: riverlog is single-node.
- Idempotent producers and transactions. A retried produce can duplicate.
- Compression and compacted topics.
- Zero-copy (`sendfile`) fetches. Reads use `pread` into a buffer.
