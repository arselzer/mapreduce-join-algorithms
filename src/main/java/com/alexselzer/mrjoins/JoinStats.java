package com.alexselzer.mrjoins;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskReport;

public class JoinStats {
    private long[] jobTimes;
    private Counters counters;
    private TaskReport[] mapTasks;
    private TaskReport[] reduceTasks;

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

    public TaskReport[] getMapTasks() {
        return mapTasks;
    }

    public void setMapTasks(TaskReport[] mapTasks) {
        this.mapTasks = mapTasks;
    }

    public TaskReport[] getReduceTasks() {
        return reduceTasks;
    }

    public void setReduceTasks(TaskReport[] reduceTasks) {
        this.reduceTasks = reduceTasks;
    }
}
