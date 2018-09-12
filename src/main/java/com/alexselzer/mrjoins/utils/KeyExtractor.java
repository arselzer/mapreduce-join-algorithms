package com.alexselzer.mrjoins.utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KeyExtractor {

    public static class KeyExtractionMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String joinAttr = value.toString().split(",")[context.getConfiguration().getInt("index", 0)];

            context.write(new Text(joinAttr), value);
        }
    }

    public static class KeyExtractionIntegerMapper extends Mapper<Object, Text, LongWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String joinAttr = value.toString().split(",")[context.getConfiguration().getInt("index", 0)];

            context.write(new LongWritable(Long.parseLong(joinAttr)), value);
        }
    }
}
