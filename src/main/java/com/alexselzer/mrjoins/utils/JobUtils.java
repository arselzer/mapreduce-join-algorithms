package com.alexselzer.mrjoins.utils;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class JobUtils {
    /**
     *
     * @param job The mapred job to time
     * @param verbose Verbose output
     * @return The time taken in nanoseconds or 0 if the job has failed
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static long time(Job job, boolean verbose) throws InterruptedException, IOException, ClassNotFoundException {
        long startTime = System.nanoTime();
        boolean success = job.waitForCompletion(verbose);
        long endTime = System.nanoTime();

        long diff = endTime - startTime;

        //System.out.printf("Time taken: %.3f ms\n", diff / 1000000.0);

        if (!success) {
            return 0;
        }

        return diff;
    }
}
