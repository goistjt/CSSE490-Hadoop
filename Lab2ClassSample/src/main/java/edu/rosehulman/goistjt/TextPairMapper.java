package edu.rosehulman.goistjt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TextPairMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        TextPair textPair = new TextPair(tokens[0], tokens[1]);
        context.write(textPair, new IntWritable(1));
    }
}
