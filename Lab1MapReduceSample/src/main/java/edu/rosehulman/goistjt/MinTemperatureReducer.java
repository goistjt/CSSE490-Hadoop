package edu.rosehulman.goistjt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Jeremiah Goist on 12/3/2016.
 */
public class MinTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int minValue = Integer.MAX_VALUE;
        for(IntWritable val : values) {
            minValue = Math.min(val.get(), minValue);
        }
        context.write(key, new IntWritable(minValue));
    }
}
