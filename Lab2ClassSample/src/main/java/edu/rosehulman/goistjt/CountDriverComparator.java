package edu.rosehulman.goistjt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountDriverComparator extends Configured implements Tool{

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Custom Comparator with custom writable");

        job.setJarByClass(CountDriver.class);
        job.setSortComparatorClass(TextPairComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TextPairMapper.class);
        job.setReducerClass(TextPairReducer.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new CountDriverComparator(), args);
        System.exit(exitCode);
    }

}
