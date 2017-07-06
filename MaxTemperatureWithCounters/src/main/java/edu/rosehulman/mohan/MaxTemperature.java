package edu.rosehulman.mohan;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.app.job.Task;


public class MaxTemperature{

	enum Temperature{
		POSITIVE,
		NEGATIVE
	}

	public static void main(String args[]) throws Exception{


		if(args.length !=2 ){
			System.err.println("Usage: Max Temperature <input path> <outputh path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max Temperature");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		
		job.waitForCompletion(true);

		Counters counter = job.getCounters();
		long positive = counter.findCounter(MaxTemperature.Temperature.POSITIVE).getValue();
		System.out.println("Number of positive temperatures : "+positive);
		long negative = counter.findCounter(MaxTemperature.Temperature.NEGATIVE).getValue();
		System.out.println("Number of negative temperatures : "+negative);
		long total = counter.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
		System.out.println("Number of total map records : "+total);

	}

}