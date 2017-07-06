package edu.rosehulman.goistjt;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinGradesMapper extends Mapper<LongWritable, Text, TaggedText, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] details = value.toString().split(",");
        for (int i = 0; i < details.length; i++) {
            details[i] = details[i].trim();
        }
        context.write(new TaggedText(details[1], 1), new Text(details[0] + ',' + details[2]));
    }
}
