package edu.rosehulman.goistjt;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinSprintMapper extends Mapper<LongWritable, Text, IntPair, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] details = value.toString().split(",");
        context.write(new IntPair(Integer.valueOf(details[0]), 0), new Text(details[1]));
    }
}
