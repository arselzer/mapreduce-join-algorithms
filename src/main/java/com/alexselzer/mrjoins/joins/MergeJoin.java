package com.alexselzer.mrjoins.joins;

import com.alexselzer.mrjoins.Join;
import com.alexselzer.mrjoins.JoinConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
    private Job keyExtracterLeft;
    private Job keyExtracterRight;
    private Job mergeJob;
    private boolean extractKeys;

    @Override
    public void init(JoinConfig config, String name) throws IOException {
        init(config, name, true);
    }

    public void init(JoinConfig config, String name, boolean extractKeys) throws IOException {
        this.extractKeys = extractKeys;

        // Configure an inner join of two inputs
        String joinExpression = "";

        if (extractKeys) {
            Path tempFile1 = new Path("temp_" + config.getInputs()[0].getName());
            Path tempFile2 = new Path("temp_" + config.getInputs()[1].getName());

            Configuration keyExtractorLeftConf = new Configuration();
            keyExtractorLeftConf.setInt("index", config.getIndices()[0]);
            Configuration keyExtractorRightConf = new Configuration();
            keyExtractorLeftConf.setInt("index", config.getIndices()[1]);

            keyExtracterLeft = Job.getInstance(keyExtractorLeftConf,
                    (name == null ? "Merge Join" : name) + " key extractor left");
            keyExtracterRight = Job.getInstance(keyExtractorRightConf,
                    (name == null ? "Merge Join" : name) + " key extractor right");

            keyExtracterLeft.setJarByClass(MergeJoin.class);
            keyExtracterRight.setJarByClass(MergeJoin.class);

            FileInputFormat.setInputPaths(keyExtracterLeft, config.getInputs()[0]);
            FileInputFormat.setInputPaths(keyExtracterRight, config.getInputs()[1]);

            FileOutputFormat.setOutputPath(keyExtracterLeft, tempFile1);
            FileOutputFormat.setOutputPath(keyExtracterRight, tempFile2);

            keyExtracterLeft.setMapperClass(KeyExtractionMapper.class);
            keyExtracterRight.setMapperClass(KeyExtractionMapper.class);
            keyExtracterLeft.setNumReduceTasks(0);
            keyExtracterRight.setNumReduceTasks(0);

            keyExtracterLeft.setMapOutputKeyClass(Text.class);
            keyExtracterLeft.setMapOutputValueClass(Text.class);

            keyExtracterLeft.setOutputKeyClass(Text.class);
            keyExtracterLeft.setOutputValueClass(Text.class);

            keyExtracterRight.setMapOutputKeyClass(Text.class);
            keyExtracterRight.setMapOutputValueClass(Text.class);

            keyExtracterRight.setOutputKeyClass(Text.class);
            keyExtracterRight.setOutputValueClass(Text.class);

            // Set the inputs to the merge job to the output file
            joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class,
                    tempFile1, tempFile2);
        }
        else {
            // Set the inputs to the merge job to the direct inputs if the data is already in
            // key-value format
            joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class,
                    config.getInputs()[0], config.getInputs()[1]);
        }

        Configuration mergeJobConf = new Configuration();

        mergeJobConf.set(CompositeInputFormat.JOIN_EXPR, joinExpression);
        //conf.set(CompositeInputFormat.JOIN_COMPARATOR, IntWritable.Comparator.class.getName());

        // Set the key - value separator (default = tab)
        mergeJobConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", SEPARATOR);

        // Disable the splitting of files so that each split corresponds to one file/table and there
        // is an equal number if splits for both tables being joined
        mergeJobConf.set(FileInputFormat.SPLIT_MINSIZE, Long.MAX_VALUE + "");

        mergeJob = Job.getInstance(mergeJobConf, name == null ? "Hash Join" : name);

        mergeJob.setJarByClass(MergeJoin.class);

        FileOutputFormat.setOutputPath(mergeJob, config.getOutput());

        mergeJob.setInputFormatClass(CompositeInputFormat.class);

        mergeJob.setMapperClass(MergeJoinMapper.class);
        mergeJob.setNumReduceTasks(0);

        mergeJob.setMapOutputKeyClass(Text.class);
        mergeJob.setMapOutputValueClass(Text.class);

        mergeJob.setOutputKeyClass(Text.class);
        mergeJob.setOutputValueClass(Text.class);
    }

    public static class KeyExtractionMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String joinAttr = value.toString().split(",")[context.getConfiguration().getInt("index", 0)];

            context.write(new Text(joinAttr), value);
        }
    }

    public static class MergeJoinMapper extends Mapper<Text, TupleWritable, Text, Text>{
        public void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
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

    @Override
    public Job getMergeJob() {
        return mergeJob;
    }

    @Override
    public boolean run(boolean verbose) throws InterruptedException, IOException, ClassNotFoundException {
        if (extractKeys) {
            keyExtracterLeft.waitForCompletion(verbose);
            keyExtracterRight.waitForCompletion(verbose);
        }

        return mergeJob.waitForCompletion(verbose);
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

        Job job = join.getMergeJob();

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}