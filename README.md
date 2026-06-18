[README.md](https://github.com/user-attachments/files/29081662/README.md)
# MAP-REDUCE-SIMULATION
A simple MapReduce programming model used to reduce the dataset. 
# Distributed Mini-MapReduce Simulation (Java)

A local, multithreaded simulation of a real-world MapReduce big-data engine
(à la Hadoop/Spark), built with plain Java `java.util.concurrent` primitives.
It parses log files in parallel and computes two metrics:

1. **Word frequency** across all log messages
2. **Log-level distribution** (INFO / WARN / ERROR / DEBUG / FATAL)

## Architecture

```
 Input logs (server1.log, server2.log, server3.log)
            │
            ▼
      ┌───────────┐
      │   SPLIT   │  driver divides all lines into N chunks
      └─────┬─────┘
            ▼
   ┌─────────────────────────┐
   │        MAP PHASE         │  N Mapper threads (ExecutorService)
   │  MapTask x N  ──────────▶│  each parses its chunk, emits
   │                          │  (word, 1) and (LEVEL:x, 1) pairs
   └─────────────┬────────────┘
                 ▼
   ConcurrentHashMap<String, AtomicLong>   <- shared intermediate store
                 │
        ── synchronization barrier ──      (Future.get() on every Mapper)
                 │
                 ▼
   ┌─────────────────────────┐
   │      REDUCE PHASE        │  M Reducer threads (ExecutorService)
   │  ReduceTask x M ────────▶│  each owns a hashed partition of keys,
   │                          │  aggregates into the final result map
   └─────────────┬────────────┘
                 ▼
   ConcurrentHashMap<String, Long>          <- final result store
                 │
                 ▼
            Console report
```

### Key design points

- **Thread-safe intermediate aggregation**: every Mapper thread writes into
  one shared `ConcurrentHashMap<String, AtomicLong>`. Keys are created with
  `computeIfAbsent` (safe under concurrent access — only one `AtomicLong` is
  ever created per key) and incremented with `AtomicLong.incrementAndGet()`,
  so no explicit locks are needed even with many mapper threads writing to
  the same map concurrently.
- **Phase barrier**: the driver (`MapReduceJob`) submits all `MapTask`s to an
  `ExecutorService` and blocks on every returned `Future` before starting the
  Reduce phase — mirroring the hard barrier between Map and Reduce in real
  MapReduce engines (no reducer can start until all mappers finish).
- **Partitioning**: intermediate keys are split across reducers by
  `hash(key) % numReducers`, the same partitioning strategy real engines use
  to route each key deterministically to exactly one reducer.
- **Parallel reduce**: since partitions never overlap, all Reducer threads
  run concurrently with zero contention, writing final sums into a second
  `ConcurrentHashMap<String, Long>`.

## Project structure

```
mapreduce-sim/
├── data/
│   ├── server1.log        # sample log files (synthetically generated)
│   ├── server2.log
│   └── server3.log
└── src/main/java/com/sim/mapreduce/
    ├── Main.java           # entry point / CLI
    ├── MapReduceJob.java   # orchestrates split -> map -> barrier -> reduce -> report
    ├── MapTask.java        # Callable<Integer>; one Map task / thread
    ├── ReduceTask.java     # Callable<Integer>; one Reduce task / thread
    └── KeyValue.java       # simple intermediate (key, value) pair type
```

## Build & run

Requires JDK 11+ (uses `List.of`, `text blocks`-free syntax compatible with
older versions too if you lower the source level).

```bash
# from the mapreduce-sim/ directory
javac -d out src/main/java/com/sim/mapreduce/*.java
java -cp out com.sim.mapreduce.Main
```

This runs with the default 4 mapper threads / 3 reducer threads against the
sample logs in `data/`.

### Custom thread counts or input files

```bash
java -cp out com.sim.mapreduce.Main <numMappers> <numReducers> <logFile1> <logFile2> ...

# example: 8 mappers, 4 reducers, single custom file
java -cp out com.sim.mapreduce.Main 8 4 data/server1.log
```

## Sample output

```
Distributed Mini-MapReduce Simulation
Mappers: 4 | Reducers: 3
Input files: [data/server1.log, data/server2.log, data/server3.log]
Loaded 3600 log lines from 3 file(s).

=== MAP PHASE (4 mapper threads) ===
  [Mapper-2] processed 900 lines on pool-1-thread-3
  [Mapper-0] processed 900 lines on pool-1-thread-1
  [Mapper-1] processed 900 lines on pool-1-thread-2
  [Mapper-3] processed 900 lines on pool-1-thread-4
Map phase complete. 3600 lines processed, 612 distinct intermediate keys.

=== REDUCE PHASE (3 reducer threads) ===
  [Reducer-1] aggregated 198 keys on pool-2-thread-2
  [Reducer-0] aggregated 203 keys on pool-2-thread-1
  [Reducer-2] aggregated 211 keys on pool-2-thread-3
Reduce phase complete. 612 keys aggregated into final result store.

=== RESULTS ===

-- Log Level Counts --
  INFO               1842
  WARN               712
  ERROR              542
  DEBUG               498
  FATAL                 6

-- Top 15 Words --
  ...

Total distinct intermediate keys: 612
Total distinct result keys:        612
Elapsed time:                       41 ms
```

## Possible extensions

- Swap the in-memory intermediate store for files-on-disk per mapper +
  an explicit shuffle/merge step (closer to real Hadoop spill files).
- Add a `Combiner` step inside `MapTask` to pre-aggregate before reduce.
- Generalize `MapTask`/`ReduceTask` with generic `<K, V>` types and a
  pluggable `Mapper<K,V>` / `Reducer<K,V>` functional interface so the
  engine can run arbitrary jobs, not just log metrics.
