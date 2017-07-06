package edu.rosehulman.goistjt;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AverageMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] details = value.toString().split("\\t");
        context.write(new Text(String.format("%s\t%s", details[1], details[2])), new DoubleWritable(Double.valueOf(details[3])));
    }
}
