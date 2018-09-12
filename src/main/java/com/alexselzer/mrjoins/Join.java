package com.alexselzer.mrjoins;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;

public interface Join {
    void init(JoinConfig config, String name) throws IOException;

    Job getMergeJob();
    boolean run(boolean verbose) throws InterruptedException, IOException, ClassNotFoundException;
}
