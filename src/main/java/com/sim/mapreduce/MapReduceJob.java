package com.sim.mapreduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Driver class that simulates a local, single-machine MapReduce engine.
 *
 * Pipeline:
 *   1. SPLIT    - input log file(s) are read and divided into chunks
 *                 (one chunk per Map task), simulating how a real
 *                 MapReduce framework splits input across data blocks.
 *   2. MAP      - each chunk is processed in parallel by a pool of
 *                 Mapper threads, emitting intermediate (key, count)
 *                 pairs into a shared ConcurrentHashMap<String, AtomicLong>.
 *   3. BARRIER  - the driver waits (via Future.get()) for ALL Map tasks
 *                 to complete before starting Reduce, exactly like the
 *                 synchronization barrier between map and reduce phases
 *                 in real engines (no reducer may start on a key until
 *                 all mappers have finished emitting).
 *   4. PARTITION/REDUCE - intermediate keys are partitioned by hash across
 *                 Reducer threads, which aggregate final counts into a
 *                 second ConcurrentHashMap<String, Long> (the result store).
 *   5. REPORT   - final results are sorted and printed/written to disk.
 */
public class MapReduceJob {

    private final int numMappers;
    private final int numReducers;

    // Shared intermediate store written by all Mapper threads concurrently.
    private final ConcurrentHashMap<String, AtomicLong> intermediateStore = new ConcurrentHashMap<>();

    // Shared final result store written by all Reducer threads concurrently.
    private final ConcurrentHashMap<String, Long> resultStore = new ConcurrentHashMap<>();

    public MapReduceJob(int numMappers, int numReducers) {
        this.numMappers = numMappers;
        this.numReducers = numReducers;
    }

    public void run(List<Path> inputFiles) throws IOException, InterruptedException, ExecutionException {
        long startTime = System.currentTimeMillis();

        List<String> allLines = readAllLines(inputFiles);
        System.out.printf("Loaded %d log lines from %d file(s).%n", allLines.size(), inputFiles.size());

        runMapPhase(allLines);
        runReducePhase();

        long elapsed = System.currentTimeMillis() - startTime;
        printReport(elapsed);
    }

    private List<String> readAllLines(List<Path> inputFiles) throws IOException {
        List<String> allLines = new ArrayList<>();
        for (Path file : inputFiles) {
            allLines.addAll(Files.readAllLines(file));
        }
        return allLines;
    }

    /** Splits lines into numMappers roughly-equal chunks and runs them in parallel. */
    private void runMapPhase(List<String> allLines) throws InterruptedException, ExecutionException {
        System.out.println("\n=== MAP PHASE (" + numMappers + " mapper threads) ===");

        List<List<String>> chunks = splitIntoChunks(allLines, numMappers);
        ExecutorService mapPool = Executors.newFixedThreadPool(numMappers);

        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < chunks.size(); i++) {
                MapTask task = new MapTask(i, chunks.get(i), intermediateStore);
                futures.add(mapPool.submit(task));
            }

            // --- Synchronization barrier: block until every Map task finishes ---
            int totalProcessed = 0;
            for (Future<Integer> f : futures) {
                totalProcessed += f.get();
            }
            System.out.printf("Map phase complete. %d lines processed, %d distinct intermediate keys.%n",
                    totalProcessed, intermediateStore.size());
        } finally {
            mapPool.shutdown();
        }
    }

    /** Partitions intermediate keys across numReducers and runs them in parallel. */
    private void runReducePhase() throws InterruptedException, ExecutionException {
        System.out.println("\n=== REDUCE PHASE (" + numReducers + " reducer threads) ===");

        List<List<String>> partitions = ReduceTask.partition(intermediateStore, numReducers);
        ExecutorService reducePool = Executors.newFixedThreadPool(numReducers);

        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < partitions.size(); i++) {
                ReduceTask task = new ReduceTask(i, partitions.get(i), intermediateStore, resultStore);
                futures.add(reducePool.submit(task));
            }

            int totalReduced = 0;
            for (Future<Integer> f : futures) {
                totalReduced += f.get();
            }
            System.out.printf("Reduce phase complete. %d keys aggregated into final result store.%n", totalReduced);
        } finally {
            reducePool.shutdown();
        }
    }

    private List<List<String>> splitIntoChunks(List<String> lines, int numChunks) {
        List<List<String>> chunks = new ArrayList<>();
        int total = lines.size();
        int chunkSize = (int) Math.ceil((double) total / numChunks);

        for (int i = 0; i < total; i += chunkSize) {
            chunks.add(lines.subList(i, Math.min(i + chunkSize, total)));
        }
        return chunks;
    }

    private void printReport(long elapsedMs) {
        System.out.println("\n=== RESULTS ===");

        // Log level breakdown
        System.out.println("\n-- Log Level Counts --");
        resultStore.entrySet().stream()
                .filter(e -> e.getKey().startsWith("LEVEL:"))
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .forEach(e -> System.out.printf("  %-18s %d%n",
                        e.getKey().replace("LEVEL:", ""), e.getValue()));

        // Top 15 words
        System.out.println("\n-- Top 15 Words --");
        resultStore.entrySet().stream()
                .filter(e -> e.getKey().startsWith("WORD:"))
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(15)
                .forEach(e -> System.out.printf("  %-18s %d%n",
                        e.getKey().replace("WORD:", ""), e.getValue()));

        System.out.printf("%nTotal distinct intermediate keys: %d%n", intermediateStore.size());
        System.out.printf("Total distinct result keys:        %d%n", resultStore.size());
        System.out.printf("Elapsed time:                       %d ms%n", elapsedMs);
    }

    /** Exposes final results, e.g. for writing to a file or for unit tests. */
    public Map<String, Long> getResults() {
        return resultStore;
    }
}
