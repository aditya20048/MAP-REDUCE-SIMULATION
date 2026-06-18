package com.sim.mapreduce;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A single Map task. In real MapReduce engines (Hadoop/Spark), each Mapper
 * is handed a "split" of the input and independently emits intermediate
 * (key, value) pairs. Here, each MapTask is handed a chunk of log lines
 * (its split) and runs on its own worker thread inside an ExecutorService.
 *
 * Two metrics are emitted per line:
 *   1. Word frequency      -> key = the lowercase word
 *   2. Log level frequency -> key = "LEVEL:" + level   (INFO / WARN / ERROR / DEBUG)
 *
 * Thread-safety: every MapTask shares ONE ConcurrentHashMap<String, AtomicLong>
 * (the "intermediate store"). Instead of locking the whole map, each thread
 * does a lock-free atomic increment on the AtomicLong bucket for its key,
 * via computeIfAbsent + AtomicLong.incrementAndGet. This is the thread-safety
 * mechanism referenced in the project summary.
 */
public class MapTask implements Callable<Integer> {

    // Matches a log line like: 2024-05-01 10:23:11 ERROR Connection refused
    private static final Pattern LOG_LEVEL_PATTERN =
            Pattern.compile("\\b(INFO|WARN|ERROR|DEBUG|TRACE|FATAL)\\b");

    private static final Pattern WORD_PATTERN = Pattern.compile("[a-zA-Z]+");

    private final int taskId;
    private final List<String> lines;
    private final ConcurrentHashMap<String, AtomicLong> intermediateStore;

    public MapTask(int taskId, List<String> lines, ConcurrentHashMap<String, AtomicLong> intermediateStore) {
        this.taskId = taskId;
        this.lines = lines;
        this.intermediateStore = intermediateStore;
    }

    /**
     * Executes the Map phase for this chunk and returns the number of
     * lines processed (used for a simple progress/throughput report).
     */
    @Override
    public Integer call() {
        int processed = 0;
        for (String line : lines) {
            if (line == null || line.isBlank()) {
                continue;
            }
            emitLogLevel(line);
            emitWordCounts(line);
            processed++;
        }
        System.out.printf("  [Mapper-%d] processed %d lines on %s%n",
                taskId, processed, Thread.currentThread().getName());
        return processed;
    }

    private void emitLogLevel(String line) {
        Matcher m = LOG_LEVEL_PATTERN.matcher(line);
        if (m.find()) {
            String level = "LEVEL:" + m.group(1);
            emit(level);
        } else {
            emit("LEVEL:UNKNOWN");
        }
    }

    private void emitWordCounts(String line) {
        Matcher m = WORD_PATTERN.matcher(line);
        while (m.find()) {
            String word = m.group().toLowerCase();
            if (word.length() < 3) {
                continue; // skip tiny noise tokens (a, an, of, ok, id, etc.)
            }
            if (STOP_WORDS.contains(word)) {
                continue;
            }
            emit("WORD:" + word);
        }
    }

    /**
     * Thread-safe emit into the shared intermediate map.
     * computeIfAbsent guarantees only one AtomicLong is ever created per key
     * even under concurrent access from multiple Mapper threads; the
     * subsequent incrementAndGet is itself atomic, so no external
     * synchronization/locking is required.
     */
    private void emit(String key) {
        intermediateStore.computeIfAbsent(key, k -> new AtomicLong(0))
                          .incrementAndGet();
    }

    private static final java.util.Set<String> STOP_WORDS = java.util.Set.of(
            "the", "and", "for", "are", "but", "not", "you", "all", "can",
            "her", "was", "one", "our", "out", "day", "get", "has", "him",
            "his", "how", "man", "new", "now", "old", "see", "two", "way",
            "who", "boy", "did", "its", "let", "put", "say", "she", "too",
            "use", "with", "from", "this", "that", "have", "been", "were"
    );
}
