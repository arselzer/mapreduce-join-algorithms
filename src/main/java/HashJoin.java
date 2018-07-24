import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HashJoin {
    private static int index1;
    private static int index2;

    public static class HashJoinLeftMapper extends Mapper<Object, Text, Text, JoinTuple>{
        public void map(Object o, Text text, Context context) throws IOException, InterruptedException {
            System.out.printf("Mapping %s (left)\n", text);
            String joinAttr = text.toString().split(",")[index1];
            context.write(new Text(joinAttr), new JoinTuple(0, text));
            //System.out.printf("Wrote %s\n",  new JoinTuple(0, text));
        }
    }

    public static class HashJoinRightMapper extends Mapper<Object, Text, Text, JoinTuple> {
        public void map(Object o, Text text, Context context) throws IOException, InterruptedException {
            System.out.printf("Mapping %s (right)\n", text);
            String joinAttr = text.toString().split(",")[index2];
            context.write(new Text(joinAttr), new JoinTuple(1, text));
            //System.out.printf("Wrote %s\n",  new JoinTuple(1, text));
        }
    }

    public static class HashJoinReducer extends Reducer<Text, JoinTuple, Text, Text> {
        public void reduce(Text key, Iterable<JoinTuple> values, Context context) throws IOException, InterruptedException {
            // The values have to be cached since the iterator can only be iterated through once
            List<JoinTuple> tuples = new ArrayList<>();
            for (JoinTuple t : values) {
                // https://cornercases.wordpress.com/2011/08/18/hadoop-object-reuse-pitfall-all-my-reducer-values-are-the-same/
                // Clone the JoinTuple object and its Writables because otherwise all values will be the same...
                tuples.add(new JoinTuple(t));
            }

            //System.out.printf("Key: %s, Tuples: %s", key, tuples);
            for (JoinTuple t1 : tuples) {
                for (JoinTuple t2: tuples) {
                    //System.out.printf("Reducer: processing %s, %s\n", t1, t2);
                    if (t1.tableIndex.get() != t2.tableIndex.get() && t1.tableIndex.get() == 0) {
                        //System.out.printf("Reducer: %s, %s, join attrs: %s, %s\n", t1, t2, joinAttr1, joinAttr2);

                        context.write(key, new Text(t1.getTuple().toString() + "," + t2.getTuple().toString()));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 5) {
            System.err.println("Usage: HashJoin.jar [input1] [index1] [input2] [index2] [output]");
            System.exit(1);
        }

        Path input1 = new Path(args[0]);
        Path input2 = new Path(args[2]);
        Path output = new Path(args[4]);

        index1 = Integer.parseInt(args[1]);
        index2 = Integer.parseInt(args[3]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hash Join");

        job.setJarByClass(HashJoin.class);

        MultipleInputs.addInputPath(job, input1, TextInputFormat.class, HashJoinLeftMapper.class);
        MultipleInputs.addInputPath(job, input2, TextInputFormat.class, HashJoinRightMapper.class);
        FileOutputFormat.setOutputPath(job, output);

        job.setReducerClass(HashJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinTuple.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
