import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HashJoin {
    public static class HashJoinMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object o, Text text, Context context) throws IOException {

        }
    }

    public static class HashJoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {

        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hash Join");
        job.setJarByClass(HashJoin.class);
        job.setMapperClass(HashJoinMapper.class);
        job.setReducerClass(HashJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    }
}
