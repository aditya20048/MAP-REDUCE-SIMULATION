package com.sim.mapreduce;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single Reduce task. In real MapReduce engines, the shuffle phase
 * partitions intermediate keys across Reducers (typically by
 * hash(key) % numReducers), and each Reducer aggregates only the keys
 * in its partition. This class simulates that: it is handed a List of
 * keys (its partition) that all hashed to the same reducer bucket, reads
 * their counts from the shared intermediate store, and writes the final
 * aggregated result into a shared, thread-safe result map.
 *
 * Because no two ReduceTasks are ever given overlapping keys, and the
 * underlying structure is a ConcurrentHashMap, the reduce step is safe
 * to run fully in parallel across threads without any manual locking.
 */
public class ReduceTask implements Callable<Integer> {

    private final int taskId;
    private final List<String> partitionKeys;
    private final ConcurrentHashMap<String, AtomicLong> intermediateStore;
    private final ConcurrentHashMap<String, Long> resultStore;

    public ReduceTask(int taskId,
                       List<String> partitionKeys,
                       ConcurrentHashMap<String, AtomicLong> intermediateStore,
                       ConcurrentHashMap<String, Long> resultStore) {
        this.taskId = taskId;
        this.partitionKeys = partitionKeys;
        this.intermediateStore = intermediateStore;
        this.resultStore = resultStore;
    }

    @Override
    public Integer call() {
        int reduced = 0;
        for (String key : partitionKeys) {
            AtomicLong counter = intermediateStore.get(key);
            if (counter == null) {
                continue;
            }
            // "Reduce" step: here it's a simple sum (already accumulated
            // by the mappers), but this is where a real reducer could
            // merge multiple partial sums, compute min/max/avg, etc.
            resultStore.merge(key, counter.get(), Long::sum);
            reduced++;
        }
        System.out.printf("  [Reducer-%d] aggregated %d keys on %s%n",
                taskId, reduced, Thread.currentThread().getName());
        return reduced;
    }

    /**
     * Splits the intermediate key set into N roughly-equal partitions
     * using key hashing, mimicking the partitioner step that assigns
     * each intermediate key to exactly one reducer.
     */
    public static List<List<String>> partition(
            ConcurrentHashMap<String, AtomicLong> intermediateStore, int numReducers) {

        List<List<String>> partitions = new java.util.ArrayList<>();
        for (int i = 0; i < numReducers; i++) {
            partitions.add(new java.util.ArrayList<>());
        }

        for (String key : intermediateStore.keySet()) {
            int bucket = Math.floorMod(key.hashCode(), numReducers);
            partitions.get(bucket).add(key);
        }
        return partitions;
    }
}
