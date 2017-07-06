package edu.rosehulman.goistjt;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinWorkMapper extends Mapper<LongWritable, Text, IntPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] details = value.toString().split(",");
        context.write(new IntPair(Integer.valueOf(details[2]), 1), new Text(String.format("%s %s", details[0], details[1])));
    }
}
