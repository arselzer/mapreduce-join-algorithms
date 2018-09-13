package com.alexselzer.mrjoins;

import com.alexselzer.mrjoins.joins.BroadcastJoin;
import com.alexselzer.mrjoins.joins.HashJoin;
import com.alexselzer.mrjoins.joins.MergeJoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;

public class JoinSimulation {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileSystem hdfs = FileSystem.get(new Configuration());

        int rowsStep = Integer.parseInt(args[0]);
        int steps = Integer.parseInt(args[1]);
        int repetitions = Integer.parseInt(args[2]);

        for (int i = 1; i <= steps; i++) {
            int nRows = i * rowsStep;

            DataGenerator dg = new DataGenerator(DataGenerator.KeyType.NUMERIC, nRows,
                    Arrays.asList(new DataGenerator.Attribute(20), new DataGenerator.Attribute(100),
                            new DataGenerator.Attribute(80)), repetitions);

            String meta = "(rows=" + nRows + ",repetitions=" + 2 + ")";

            Path input1 = new Path("t1_" + nRows + ".csv");
            Path input2 = new Path("t2_" + nRows + ".csv");

            FSDataOutputStream out1 = hdfs.create(input1, true);
            FSDataOutputStream out2 = hdfs.create(input2, true);

            long startTime = System.nanoTime();
            dg.write(out1, out2);
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

            JoinConfig config = new JoinConfig(inputs, indices, output, 4);

            /* Run the hash join */

            Join join = new HashJoin();
            join.init(config, "hash-join" + meta);

            time(join);

            hdfs.delete(output, true);

            /* Run the broadcast join */

            join = new BroadcastJoin();
            join.init(config, "broadcast-join" + meta);

            time(join);

            hdfs.delete(output, true);

            /* Run the merge join */

            join = new MergeJoin();
            join.init(config, "merge-join" + meta);

            time(join);

            hdfs.delete(output, true);

            hdfs.delete(input1, true);
            hdfs.delete(input2, true);
        }

        System.exit(0);
    }

    private static long time(Join join) throws InterruptedException, IOException, ClassNotFoundException {
        long startTime = System.nanoTime();
        boolean success = join.run(true);
        long endTime = System.nanoTime();

        long diff = endTime - startTime;

        System.out.printf("Time taken: %.3f ms\n", diff / 1000000.0);

        return diff;
    }
}
