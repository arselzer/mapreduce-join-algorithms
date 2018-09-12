package com.alexselzer.mrjoins.joins;

import com.alexselzer.mrjoins.Join;
import com.alexselzer.mrjoins.JoinConfig;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class SortMergeJoin implements Join {
    @Override
    public void init(JoinConfig config, String name) throws IOException {

    }

    @Override
    public void init(JoinConfig config, String name, boolean extractKeys) throws IOException {

    }

    @Override
    public Job getMergeJob() {
        return null;
    }

    @Override
    public boolean run(boolean verbose) throws InterruptedException, IOException, ClassNotFoundException {
        return false;
    }
}
