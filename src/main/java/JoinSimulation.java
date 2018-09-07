import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;

public class JoinSimulation {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        generateData();

        Path input1 = new Path("t1_1000000.csv");
        Path input2 = new Path("t2_1000000.csv");
        Path output = new Path("simulation_output.csv");

        int index1 = 0;
        int index2 = 0;

        Path[] inputs = {input1, input2};
        Integer[] indices = {index1, index2};

        JoinConfig config = new JoinConfig(inputs, indices, output);

        Join join = new HashJoin();
        join.init(config);

        Job job = join.getJob();

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void generateData() {
        DataGenerator dg = new DataGenerator(DataGenerator.KeyType.NUMERIC, 10000000,
                Arrays.asList(new DataGenerator.Attribute(20), new DataGenerator.Attribute(100),
                        new DataGenerator.Attribute(80)),2);

        dg.write("1000000.csv");
    }
}
