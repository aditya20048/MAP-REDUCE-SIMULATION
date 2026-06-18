package com.sim.mapreduce;

import java.nio.file.Path;
import java.util.List;

/**
 * Entry point for the Distributed Mini-MapReduce Simulation.
 *
 * Usage:
 *   java -cp out com.sim.mapreduce.Main [numMappers] [numReducers] [logFile1] [logFile2] ...
 *
 * If no log files are given, defaults to the sample logs under ./data/.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        int numMappers = 4;
        int numReducers = 3;
        List<Path> inputFiles;

        if (args.length >= 2) {
            numMappers = Integer.parseInt(args[0]);
            numReducers = Integer.parseInt(args[1]);
        }

        if (args.length > 2) {
            inputFiles = java.util.Arrays.stream(args, 2, args.length)
                    .map(Path::of)
                    .toList();
        } else {
            inputFiles = List.of(
                    Path.of("data/server1.log"),
                    Path.of("data/server2.log"),
                    Path.of("data/server3.log")
            );
        }

        System.out.println("Distributed Mini-MapReduce Simulation");
        System.out.println("Mappers: " + numMappers + " | Reducers: " + numReducers);
        System.out.println("Input files: " + inputFiles);

        MapReduceJob job = new MapReduceJob(numMappers, numReducers);
        job.run(inputFiles);
    }
}
