package com.alexselzer.mrjoins.joins;

import com.alexselzer.mrjoins.Join;
import com.alexselzer.mrjoins.JoinConfig;
import com.alexselzer.mrjoins.utils.KeyExtractor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;
import java.util.Iterator;

public class MergeJoin implements Join {
    private static final String SEPARATOR = "\t";
    private static final String JOB_NAME = "Merge Join";

    private Job keyExtractorLeft;
    private Job keyExtractorRight;

    private Job sortLeft;
    private Job sortRight;

    private Job mergeJob;

    private boolean extractKeys;
    private boolean sort;
    private JoinConfig config;
    private String name;

    @Override
    public void init(JoinConfig config, String name) throws IOException, ClassNotFoundException, InterruptedException {
        init(config, name, true, true);
    }

    public void init(JoinConfig config, String name, boolean extractKeys, boolean sort) throws IOException, InterruptedException, ClassNotFoundException {
        this.extractKeys = extractKeys;
        this.sort = sort;
        this.config = config;
        this.name = name;
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
    public boolean run(boolean verbose) throws InterruptedException, IOException, ClassNotFoundException {
        FileSystem hdfs = FileSystem.get(new Configuration());

        // Configure an inner join of the two inputs
        String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class,
                config.getInputs()[0], config.getInputs()[1]);

        Path tempInput1 = new Path("temp_" + config.getInputs()[0].getName());
        Path tempInput2 = new Path("temp_" + config.getInputs()[1].getName());

        Path tempInputSorted1 = new Path("temp_sorted_" + config.getInputs()[0].getName());
        Path tempInputSorted2 = new Path("temp_sorted_" + config.getInputs()[1].getName());

        if (extractKeys) {
            hdfs.deleteOnExit(tempInput1);
            hdfs.deleteOnExit(tempInput2);

            Configuration keyExtractorLeftConf = new Configuration();
            keyExtractorLeftConf.setInt("index", config.getIndices()[0]);
            Configuration keyExtractorRightConf = new Configuration();
            keyExtractorRightConf.setInt("index", config.getIndices()[1]);

            keyExtractorLeft = Job.getInstance(keyExtractorLeftConf,
                    (name == null ? JOB_NAME : name) + " key extractor left");
            keyExtractorRight = Job.getInstance(keyExtractorRightConf,
                    (name == null ? JOB_NAME : name) + " key extractor right");

            keyExtractorLeft.setJarByClass(KeyExtractor.class);
            keyExtractorRight.setJarByClass(KeyExtractor.class);

            FileInputFormat.setInputPaths(keyExtractorLeft, config.getInputs()[0]);
            FileInputFormat.setInputPaths(keyExtractorRight, config.getInputs()[1]);

            FileOutputFormat.setOutputPath(keyExtractorLeft, tempInput1);
            FileOutputFormat.setOutputPath(keyExtractorRight, tempInput2);

            keyExtractorLeft.setMapperClass(KeyExtractor.KeyExtractionMapper.class);
            keyExtractorRight.setMapperClass(KeyExtractor.KeyExtractionMapper.class);
            keyExtractorLeft.setNumReduceTasks(0);
            keyExtractorRight.setNumReduceTasks(0);

            keyExtractorLeft.setMapOutputKeyClass(Text.class);
            keyExtractorLeft.setMapOutputValueClass(Text.class);

            keyExtractorLeft.setOutputKeyClass(Text.class);
            keyExtractorLeft.setOutputValueClass(Text.class);

            keyExtractorRight.setMapOutputKeyClass(Text.class);
            keyExtractorRight.setMapOutputValueClass(Text.class);

            keyExtractorRight.setOutputKeyClass(Text.class);
            keyExtractorRight.setOutputValueClass(Text.class);

            // Set the inputs of the merge job to the temp output file
            joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class,
                    tempInput1, tempInput2);

            keyExtractorLeft.waitForCompletion(verbose);
            keyExtractorRight.waitForCompletion(verbose);
        }

        if (sort) {
            hdfs.deleteOnExit(tempInputSorted1);
            hdfs.deleteOnExit(tempInputSorted2);

            Path partitionFile = new Path("tmp_partitions");

            Configuration sortLeftConf = new Configuration();
            Configuration sortRightConf = new Configuration();

            sortLeft = Job.getInstance(sortLeftConf,
                    (name == null ? JOB_NAME : name) + " sort left");
            sortRight = Job.getInstance(sortRightConf,
                    (name == null ? JOB_NAME : name) + " sort right");

            if (extractKeys) {
                // If the keys had to be extracted first use the newly created temp files as input
                FileInputFormat.setInputPaths(sortLeft, tempInput1);
                FileInputFormat.setInputPaths(sortRight, tempInput2);
            }
            else {
                // Otherwise use the actual input files
                FileInputFormat.setInputPaths(sortLeft, config.getInputs()[0]);
                FileInputFormat.setInputPaths(sortRight, config.getInputs()[1]);
            }

            sortLeft.setInputFormatClass(KeyValueTextInputFormat.class);
            sortRight.setInputFormatClass(KeyValueTextInputFormat.class);

            sortLeft.setMapOutputKeyClass(Text.class);
            sortRight.setMapOutputKeyClass(Text.class);

            TotalOrderPartitioner.setPartitionFile(sortLeft.getConfiguration(), partitionFile);
            TotalOrderPartitioner.setPartitionFile(sortRight.getConfiguration(), partitionFile);

            InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
            InputSampler.writePartitionFile(sortLeft, sampler);

            sortLeft.setPartitionerClass(TotalOrderPartitioner.class);
            sortRight.setPartitionerClass(TotalOrderPartitioner.class);

            FileOutputFormat.setOutputPath(sortLeft, tempInputSorted1);
            FileOutputFormat.setOutputPath(sortRight, tempInputSorted2);

            // No mappers and reducers are set - we only need the shuffle phase and sorting

            // Set the merge join input to the newly created sorted temp files:
            joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class,
                    tempInput1, tempInput2);

            sortLeft.waitForCompletion(verbose);
            sortRight.waitForCompletion(verbose);
        }

        Configuration mergeJobConf = new Configuration();

        mergeJobConf.set(CompositeInputFormat.JOIN_EXPR, joinExpression);
        //conf.set(CompositeInputFormat.JOIN_COMPARATOR, IntWritable.Comparator.class.getName());

        // Set the key - value separator (default = tab)
        mergeJobConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", SEPARATOR);

        // Disable the splitting of files so that each split corresponds to one file/table and there
        // is an equal number if splits for both tables being joined
        mergeJobConf.set(FileInputFormat.SPLIT_MINSIZE, Long.MAX_VALUE + "");

        mergeJob = Job.getInstance(mergeJobConf, name == null ? "Merge Join" : name);

        mergeJob.setJarByClass(MergeJoin.class);

        FileOutputFormat.setOutputPath(mergeJob, config.getOutput());

        mergeJob.setInputFormatClass(CompositeInputFormat.class);

        mergeJob.setMapperClass(MergeJoinMapper.class);
        mergeJob.setNumReduceTasks(0);

        mergeJob.setMapOutputKeyClass(Text.class);
        mergeJob.setMapOutputValueClass(Text.class);

        mergeJob.setOutputKeyClass(Text.class);
        mergeJob.setOutputValueClass(Text.class);

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
        join.init(config, JOB_NAME);

        join.run(true);
    }
}