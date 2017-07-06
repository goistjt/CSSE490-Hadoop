package edu.rosehulman.mohan;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TextPairMapper extends Mapper<LongWritable,Text,TextPair,IntWritable>{

public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String[] tokens = value.toString().split(",");
	    String firstName = tokens[0];
	    String lastName = tokens[1];
	     
	    TextPair textPair = new TextPair(firstName,lastName);
	    context.write(textPair, new IntWritable(1));
	}

}