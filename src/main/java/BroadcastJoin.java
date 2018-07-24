import javafx.scene.control.cell.MapValueFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileVisitOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BroadcastJoin {
    private static int index1;
    private static int index2;

    public static class BroadcastJoinMapper extends Mapper<Object, Text, Text, Text>{
        Map<String, String> map = new HashMap<>();

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            URI[] cachedFiles = context.getCacheFiles();

            if (cachedFiles.length == 0) {
                System.err.println("Error: There are no cached files");
            }

            System.out.printf("Cached file: %s\n", cachedFiles[0]);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fs.open(new Path(cachedFiles[0]))));

            String line = reader.readLine();
            while (line != null) {
                String joinAttr = line.split(",")[index1];
                map.put(joinAttr, line);

                line = reader.readLine();
            }

            reader.close();
        }

        public void map(Object o, Text text, Context context) throws IOException, InterruptedException {
            System.out.printf("Mapping %s (right)\n", text);
            String joinAttr = text.toString().split(",")[index1];
            if (map.containsKey(joinAttr)) {
                context.write(new Text(joinAttr), new Text(map.get(joinAttr) + "," + text.toString()));
            }
            //System.out.printf("Wrote %s\n",  new JoinTuple(0, text));
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
        Job job = Job.getInstance(conf, "Broadcast Join");

        job.setJarByClass(BroadcastJoin.class);

        // Add the first file to the distributed cache - sending it to all mappers
        job.addCacheFile(input1.toUri());

        FileInputFormat.setInputPaths(job, input2);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(BroadcastJoinMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
