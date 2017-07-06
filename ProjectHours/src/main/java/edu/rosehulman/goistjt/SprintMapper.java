package edu.rosehulman.goistjt;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SprintMapper extends Mapper<LongWritable, Text, SprintTriple, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        SprintTriple sprintTriple = new SprintTriple(tokens[0], tokens[1], Integer.valueOf(tokens[2]));
        context.write(sprintTriple, new DoubleWritable(Double.valueOf(tokens[3])));
    }
}
