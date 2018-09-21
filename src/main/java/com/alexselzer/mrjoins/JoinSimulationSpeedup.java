package com.alexselzer.mrjoins;

import com.alexselzer.mrjoins.joins.BroadcastJoin;
import com.alexselzer.mrjoins.joins.RepartitionJoin;
import com.alexselzer.mrjoins.joins.MergeJoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class JoinSimulationSpeedup {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileSystem hdfs = FileSystem.get(new Configuration());

        int rowsStep = Integer.parseInt(args[0]);
        int steps = Integer.parseInt(args[1]);
        int repetitions = Integer.parseInt(args[2]);
        int nReducers = Integer.parseInt(args[3]);
        double zipfSkew = 0.5;

        PrintWriter results = new PrintWriter(new FileOutputStream("results " +
                (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())) + ".csv"));

        results.println("rows,repetitions,reducers,skew,map_records_1,reduce_records_1,t_repartition,map_records_2,t_broadcast,t_merge_1_1,t_merge_1_2,t_merge_2_1,t_merge_2_2,t_merge_3,,map_records_3,t_merge");

        for (int i = 1; i <= steps; i++) {
            int nRows = i * rowsStep;

            DataGenerator dg = new DataGenerator(DataGenerator.KeyType.NUMERIC, nRows,
                    Arrays.asList(new DataGenerator.Attribute(20), new DataGenerator.Attribute(100),
                            new DataGenerator.Attribute(80)), repetitions);

            results.write(nRows + "," + repetitions + "," + nReducers + "," + zipfSkew);

            String meta = "(rows=" + nRows + ",repetitions=" + repetitions + ",skew=" + zipfSkew + ")";

            Path input1 = new Path("t1_" + nRows + ".csv");
            Path input2 = new Path("t2_" + nRows + ".csv");

            FSDataOutputStream out1 = hdfs.create(input1, true);
            FSDataOutputStream out2 = hdfs.create(input2, true);

            long startTime = System.nanoTime();
            dg.writeZipf(out1, out2, zipfSkew);
            long endTime = System.nanoTime();

            long diff = endTime - startTime;

            System.out.printf("Data generated: %.3f ms\n", diff / 1000000.0);

            out1.close();
            out2.close();

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

            join.run(true);

            long mapRecords = join.getJoinStats().getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
            long reduceRecords = join.getJoinStats().getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();

            results.write("," + mapRecords + "," + reduceRecords + "," + join.getJoinStats().getJobTimes()[0]);

            hdfs.delete(output, true);

            /* Run the broadcast join */

            join = new BroadcastJoin();
            join.init(config, "broadcast-join" + meta);

            join.run(true);

            mapRecords = join.getJoinStats().getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();

            results.write("," + mapRecords + "," + join.getJoinStats().getJobTimes()[0]);

            hdfs.delete(output, true);

            /* Run the merge join */

            join = new MergeJoin();
            join.init(config, "merge-join" + meta);

            join.run(true);

            mapRecords = join.getJoinStats().getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();

            long[] t = join.getJoinStats().getJobTimes();
            results.write("," + mapRecords + "," + t[0] + "," + t[1] + "," + t[2] + "," + t[3] + "," + t[4] + ","
                    + (t[0] + t[1] + t[2] + t[3] + t[4]) + "\n");

            hdfs.delete(output, true);

            hdfs.delete(input1, true);
            hdfs.delete(input2, true);
        }

        results.close();

        System.exit(0);
    }
}
