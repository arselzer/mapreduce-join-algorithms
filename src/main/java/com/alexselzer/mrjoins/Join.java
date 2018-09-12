package com.alexselzer.mrjoins;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public interface Join {
    public void init(JoinConfig config, String name) throws IOException;
    public Job getJob();
}
