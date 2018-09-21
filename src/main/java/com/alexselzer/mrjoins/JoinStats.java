package com.alexselzer.mrjoins;

import org.apache.hadoop.mapreduce.Counters;

public class JoinStats {
    private long[] jobTimes;
    private Counters counters;

    public long[] getJobTimes() {
        return jobTimes;
    }

    public void setJobTimes(long[] jobTimes) {
        this.jobTimes = jobTimes;
    }

    public Counters getCounters() {
        return counters;
    }

    public void setCounters(Counters counters) {
        this.counters = counters;
    }
}
