import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;

public class JoinSimulation {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileSystem hdfs = FileSystem.get(new Configuration());

        DataGenerator dg = new DataGenerator(DataGenerator.KeyType.NUMERIC, 100000,
                Arrays.asList(new DataGenerator.Attribute(20), new DataGenerator.Attribute(100),
                        new DataGenerator.Attribute(80)),2);

        Path input1 = new Path("t1_10000.csv");
        Path input2 = new Path("t2_10000.csv");

        FSDataOutputStream out1 = hdfs.create(input1, true);
        FSDataOutputStream out2 = hdfs.create(input2, true);

        dg.write(out1, out2);

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

        JoinConfig config = new JoinConfig(inputs, indices, output);

        Join join = new HashJoin();
        join.init(config, "Hash Join");

        Job job = join.getJob();

        long startTime = System.nanoTime();
        boolean success = job.waitForCompletion(true);
        long endTime = System.nanoTime();

        long diff = endTime - startTime;

        System.out.printf("Time taken: %.3f ms\n", diff / 1000000.0);

        System.exit(0);
    }

    private static void generateData() {

    }
}
