import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class MergeJoin implements Join {
    private static final String SEPARATOR = "\t";
    private Job job;

    @Override
    public void init(JoinConfig config, String name) throws IOException {
        Configuration jobConf = new Configuration();

        // Configure an inner join of two inputs
        String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class,
                config.getInputs()[0], config.getInputs()[1]);
        jobConf.set(CompositeInputFormat.JOIN_EXPR, joinExpression);
        //conf.set(CompositeInputFormat.JOIN_COMPARATOR, IntWritable.Comparator.class.getName());

        // Set the key - value separator (default = tab)
        jobConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", SEPARATOR);

        // Disable the splitting of files so that each split corresponds to one file/table and there
        // is an equal number if splits for both tables being joined
        jobConf.set(FileInputFormat.SPLIT_MINSIZE, Long.MAX_VALUE + "");

        job = Job.getInstance(jobConf, name == null ? "Hash Join" : name);

        job.setJarByClass(MergeJoin.class);

        FileOutputFormat.setOutputPath(job, config.getOutput());

        job.setInputFormatClass(CompositeInputFormat.class);

        job.setMapperClass(MergeJoinMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    }

    @Override
    public Job getJob() {
        return job;
    }

    public static class MergeJoinMapper extends Mapper<Text, TupleWritable, Text, Text>{
        public void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
            //System.out.printf("Key: %s\n", key);
            StringBuilder output = new StringBuilder();
            Iterator<Writable> it = value.iterator();
            output.append(it.next());
            while (it.hasNext()) {
                output.append(",");
                output.append(it.next());
            }

            context.write(key, new Text(output.toString()));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 5) {
            System.err.println("Usage: MergeJoin.jar [input1] [index1] [input2] [index2] [output]");
            System.exit(1);
        }

        Path input1 = new Path(args[0]);
        Path input2 = new Path(args[2]);
        Path output = new Path(args[4]);

        int index1 = Integer.parseInt(args[1]);
        int index2 = Integer.parseInt(args[3]);
        Path[] inputs = {input1, input2};
        Integer[] indices = {index1, index2};

        JoinConfig config = new JoinConfig(inputs, indices, output);

        Join join = new MergeJoin();
        join.init(config, "Merge Join");

        Job job = join.getJob();

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}