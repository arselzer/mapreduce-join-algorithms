package com.alexselzer.mrjoins;

import com.alexselzer.mrjoins.joins.BroadcastJoin;
import com.alexselzer.mrjoins.joins.RepartitionJoin;
import com.alexselzer.mrjoins.joins.MergeJoin;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskReport;

import org.apache.commons.cli.*;

import java.io.*;
import java.text.SimpleDateFormat;

public class JoinSimulation {
    private static FileSystem hdfs;

    private static List<Long> getTaskTimes(TaskReport[] reports) {
        List<Long> times = new ArrayList<Long>(reports.length);

        for (TaskReport report : reports) {
            times.add(report.getFinishTime() - report.getStartTime());
        }

        return times;
    }

    private static double calcMean(List<Long> times) {
        // When running locally the TaskReports are not available and
        // an empty list is returned

        if (times.size() == 0) {
            return -1;
        }

        double sum = 0.0;
        for (long time : times) {
            sum += time;
        }

        return sum / times.size();
    }

    private static long calcMedian(List<Long> times) {
        if (times.size() == 0) {
            return -1;
        }

        List<Long> copy = new ArrayList<Long>(times);
        Collections.sort(copy);
        return copy.get(copy.size() / 2);
    }

    private static long calcMax(List<Long> times) {
        long max = -1;

        for (long val : times) {
            if (val > max) {
                max = val;
            }
        }

        return max;
    }

    private static <T extends Object> String joinList(List<T> list, String sep) {
        if (list.size() == 0) {
            return "";
        }

        StringBuilder str = new StringBuilder();
        for (int i = 0; i < list.size() - 1; i++) {
            str.append(list.get(i)).append(sep);
        }

        str.append(list.get(list.size() - 1));

        return str.toString();
    }

    private static void run(PrintWriter results, boolean repartitionJoin, boolean broadcastJoin, boolean mergeJoin,
                              long nRows, long uniqueValues, int nReducers, double zipfSkew, boolean doubleSkew, int nThreads) throws IOException, ClassNotFoundException, InterruptedException {

        DataGenerator dg = new DataGenerator(DataGenerator.KeyType.NUMERIC, nRows,
                Arrays.asList(new DataGenerator.Attribute(20), new DataGenerator.Attribute(100),
                        new DataGenerator.Attribute(80)), uniqueValues);

        results.write(nRows + "," + uniqueValues + "," + nReducers + "," + zipfSkew);

        String meta = "(rows=" + nRows + ",N=" + uniqueValues + ",skew=" + zipfSkew + ",doubleSkew=" + doubleSkew + ")";

        Path input1 = new Path("t1_" + nRows + ".csv");
        Path input2 = new Path("t2_" + nRows + ".csv");

        long startTime = System.nanoTime();
        if (!doubleSkew) {
            if (nThreads > 1) {
                dg.writeZipfParallelToHdfs(input1, input2, zipfSkew, nThreads);
            }
            else {
                FSDataOutputStream out1 = hdfs.create(input1, true);
                FSDataOutputStream out2 = hdfs.create(input2, true);

                dg.writeZipf(out1, out2, zipfSkew);

                out1.close();
                out2.close();
            }
        }
        else {
            FSDataOutputStream out1 = hdfs.create(input1, true);
            FSDataOutputStream out2 = hdfs.create(input2, true);

            dg.writeZipfBoth(out1, out2, zipfSkew);

            out1.close();
            out2.close();
        }
        long endTime = System.nanoTime();

        long diff = endTime - startTime;

        System.out.printf("Data generated(nrows=%d,nthreads=%d): %.3f ms - file size of t2: %dMB\n",
                nRows, nThreads, diff / 1000000.0,  hdfs.getContentSummary(input2).getSpaceConsumed() / 1000000);

        Path output = new Path("simulation_output");

        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        //hdfs.deleteOnExit(input1);
        //hdfs.deleteOnExit(input2);

        int index1 = 0;
        int index2 = 0;

        Path[] inputs = {input1, input2};
        Integer[] indices = {index1, index2};

        JoinConfig config = new JoinConfig(inputs, indices, output, nReducers);

        /* Run the hash join */

        Join join = new RepartitionJoin();
        join.init(config, "repartition-join" + meta);

        System.out.printf("Running %s", "repartition-join" + meta);

        join.run(true);

        long mapRecords = join.getJoinStats().getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        long reduceRecords = join.getJoinStats().getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();

        List<Long> mapTimes = getTaskTimes(join.getJoinStats().getMapTasks());
        List<Long> reduceTimes = getTaskTimes(join.getJoinStats().getReduceTasks());

        results.write("," + mapRecords + "," + reduceRecords + "," +
                joinList(mapTimes, ";") + "," + calcMedian(mapTimes) + "," + calcMean(mapTimes) + "," + calcMax(mapTimes) + "," +
                joinList(reduceTimes, ";") + "," + calcMedian(reduceTimes) + "," + calcMean(reduceTimes) + "," + calcMax(reduceTimes) + "," +
                join.getJoinStats().getJobTimes()[0]);

        hdfs.delete(output, true);

        /* Run the broadcast join */

        if (broadcastJoin) {

            join = new BroadcastJoin();
            join.init(config, "broadcast-join" + meta);

            System.out.printf("Running %s", "broadcast-join" + meta);

            join.run(true);

            mapRecords = join.getJoinStats().getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();

            mapTimes = getTaskTimes(join.getJoinStats().getMapTasks());

            results.write("," + mapRecords + "," +
                    joinList(mapTimes, ";") + "," + calcMedian(mapTimes) + "," + calcMean(mapTimes) + "," + calcMax(mapTimes) + "," +
                    join.getJoinStats().getJobTimes()[0]);

            hdfs.delete(output, true);

        }
        else {
            results.write(",,,,,,");
        }

        /* Run the merge join */

        join = new MergeJoin();
        join.init(config, "merge-join" + meta);

        System.out.printf("Running %s", "merge-join" + meta);

        join.run(true);

        mapRecords = join.getJoinStats().getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();

        mapTimes = getTaskTimes(join.getJoinStats().getMapTasks());

        long[] t = join.getJoinStats().getJobTimes();
        results.write("," + mapRecords + "," +
                joinList(mapTimes, ";") + "," + calcMedian(mapTimes) + "," + calcMean(mapTimes) + "," + calcMax(mapTimes) + "," +
                t[0] + "," + t[1] + "," + t[2] + "," + t[3] + "," + t[4] + "," +
                (t[0] + t[1] + t[2] + t[3] + t[4]) + "\n");

        hdfs.delete(output, true);

        hdfs.delete(input1, true);
        hdfs.delete(input2, true);

        results.flush();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ParseException {
        hdfs = FileSystem.get(new Configuration());

        Options options = new Options();

        options.addOption(null, "no-broadcast-join", false, "Don't perform a broadcast join");
        options.addOption(null, "no-header", false, "Write no CSV header");
        options.addOption(null, "rows", true, "The number of rows");
        options.addOption(null, "steps", true, "The number of runthroughs");
        options.addOption(null, "increment", true, "The number of rows to increase by");
        options.addOption(null, "unique-values", true, "The number of unique keys < rows");
        options.addOption(null, "unique", true, "The percentage of unique values");
        options.addOption(null, "reducers", true, "The number of reducers");
        options.addOption(null, "zipf-skew", true, "Skew");
        options.addOption(null, "double-skew", false, "Whether both tables are skewed");
        options.addOption(null, "out", true, "Results file name");
        options.addOption(null, "threads", true, "The number of threads to use for generating data");

        // We cannot use commons-cli 1.4 because it causes trouble with Hadoop already using 1.2
        CommandLineParser parser = new org.apache.commons.cli.BasicParser();

        CommandLine cmd = parser.parse(options, args);

        boolean performBroadcastJoin = true;
        boolean writeHeader = true;
        boolean doubleSkew = false;

        if (cmd.hasOption("no-broadcast-join"))
            performBroadcastJoin = false;

        if (cmd.hasOption("no-header"))
            writeHeader = false;

        if (cmd.hasOption("double-skew"))
            doubleSkew = true;

        long rows = 10000;

        if (cmd.hasOption("rows"))
            rows = Long.parseLong(cmd.getOptionValue("rows"));

        long increment = rows;

        if (cmd.hasOption("increment"))
            increment = Long.parseLong(cmd.getOptionValue("increment"));

        int steps = 1;

        if (cmd.hasOption("steps"))
            steps = Integer.parseInt(cmd.getOptionValue("steps"));

        double uniquePercentage = 0.10;
        if (cmd.hasOption("unique-values"))
            uniquePercentage = Double.parseDouble(cmd.getOptionValue("unique"));

        long uniqueValues = (long) (rows * uniquePercentage);

        if (cmd.hasOption("unique-values"))
            uniqueValues = Long.parseLong(cmd.getOptionValue("unique-values"));

        int nReducers = 4;

        if (cmd.hasOption("reducers"))
            nReducers = Integer.parseInt(cmd.getOptionValue("reducers"));

        double zipfSkew = 0.5;

        if (cmd.hasOption("zipf-skew"))
            zipfSkew = Double.parseDouble(cmd.getOptionValue("zipf-skew"));

        String fileName = "results " +
                (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())) + ".csv";

        if (cmd.hasOption("out"))
            fileName = cmd.getOptionValue("out");

        int nThreads = 1;
        if (cmd.hasOption("threads"))
            nThreads = Integer.parseInt(cmd.getOptionValue("threads"));

        PrintWriter results = new PrintWriter(new FileOutputStream(fileName));

        if (writeHeader) {
            results.println("rows,unique_values,reducers,skew," +
                    "map_records_1,reduce_records_1,mt_1,mt_med_1,mt_mu_1,mt_max_1,rt_1,rt_med_1,rt_mu_1,rt_max_1,t_repartition," +
                    "map_records_2,mt_2,mt_med_2,mt_mu_2,mt_max_2,t_broadcast," +
                    "map_records_3,mt_3,mt_med_3,mt_mu_3,mt_max_3,t_merge_1_1,t_merge_1_2,t_merge_2_1,t_merge_2_2,t_merge_3,t_merge");
        }

        for (int i = 0; i < steps; i++) {
            long nRows = rows + i * increment;

            run(results, true, performBroadcastJoin, true,
                    nRows, uniqueValues, nReducers, zipfSkew, doubleSkew, nThreads);
        }

        results.close();

        System.exit(0);
    }
}
