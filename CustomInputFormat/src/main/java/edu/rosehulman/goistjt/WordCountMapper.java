package edu.rosehulman.goistjt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        if (value.get() < 2) {
            context.getCounter(WordCountDriver.WordCount.LessThanTwo).increment(1);
        } else if (value.get() == 2) {
            context.getCounter(WordCountDriver.WordCount.EqualToTwo).increment(1);
        } else {
            context.getCounter(WordCountDriver.WordCount.GreaterThanTwo).increment(1);
        }
        context.write(key, value);
    }
}
