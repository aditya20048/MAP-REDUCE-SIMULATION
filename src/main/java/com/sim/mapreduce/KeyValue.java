package com.sim.mapreduce;

/**
 * Simple immutable intermediate key-value pair, analogous to the
 * (key, value) tuples emitted by Mappers in real MapReduce engines
 * (e.g. Hadoop's Mapper.map() output) before the shuffle/sort phase.
 */
public final class KeyValue {
    private final String key;
    private final long value;

    public KeyValue(String key, long value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }
}
