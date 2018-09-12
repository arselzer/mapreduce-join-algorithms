package com.alexselzer.mrjoins.joins;

import com.alexselzer.mrjoins.Join;
import com.alexselzer.mrjoins.JoinConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class BroadcastJoin implements Join {
    private Job job;

    @Override
    public void init(JoinConfig config, String name) throws IOException {
        Configuration jobConf = new Configuration();
        jobConf.setInt("index1", config.getIndices()[0]);
        jobConf.setInt("index2", config.getIndices()[1]);

        job = Job.getInstance(jobConf, name == null ? "Hash Join" : name);

        job.setJarByClass(BroadcastJoin.class);

        // Add the first file to the distributed cache - sending it to all mappers
        job.addCacheFile(config.getInputs()[0].toUri());

        FileInputFormat.setInputPaths(job, config.getInputs()[1]);
        FileOutputFormat.setOutputPath(job, config.getOutput());

        job.setMapperClass(BroadcastJoinMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    }

    @Override
    public void init(JoinConfig config, String name, boolean extractKeys) throws IOException {
        init(config, name, false);
    }

    public static class BroadcastJoinMapper extends Mapper<Object, Text, Text, Text>{
        Map<String, String> map = new HashMap<>();

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            URI[] cachedFiles = context.getCacheFiles();

            if (cachedFiles.length == 0) {
                System.err.println("Error: There are no cached files");
            }

            //System.out.printf("Cached file: %s\n", cachedFiles[0]);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fs.open(new Path(cachedFiles[0]))));

            String line = reader.readLine();
            while (line != null) {
                String joinAttr = line.split(",")[context.getConfiguration().getInt("index1", 0)];
                map.put(joinAttr, line);

                line = reader.readLine();
            }

            reader.close();
        }

        public void map(Object o, Text text, Context context) throws IOException, InterruptedException {
            //System.out.printf("Mapping %s (right)\n", text);
            String joinAttr = text.toString().split(",")[context.getConfiguration().getInt("index2", 0)];
            if (map.containsKey(joinAttr)) {
                context.write(new Text(joinAttr), new Text(map.get(joinAttr) + "," + text.toString()));
            }
            //System.out.printf("Wrote %s\n",  new com.alexselzer.mrjoins.JoinTuple(0, text));
        }
    }

    @Override
    public Job getMergeJob() {
        return job;
    }

    @Override
    public boolean run(boolean verbose) throws InterruptedException, IOException, ClassNotFoundException {
        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 5) {
            System.err.println("Usage: HashJoin.jar [input1] [index1] [input2] [index2] [output]");
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

        Join join = new BroadcastJoin();
        join.init(config, "Broadcast Join");

        Job job = join.getMergeJob();

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
