package edu.rosehulman.goistjt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TextPairReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {
    @Override
    protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        final int[] sum = {0};
        values.forEach(v -> sum[0] += v.get());
        context.write(key, new IntWritable(sum[0]));
    }
}
