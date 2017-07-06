package edu.rosehulman.goistjt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<IntPair, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        String sprintName = iterator.next().toString();
        String name;
        while (iterator.hasNext()) {
            name = iterator.next().toString();
            Text outVal = new Text(String.format("%s\t%s", sprintName, name));
            context.write(key.getFirst(), outVal);
        }
    }
}
