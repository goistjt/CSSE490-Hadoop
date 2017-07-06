package edu.rosehulman.goistjt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {
    enum WordCount {
        EqualToTwo,
        LessThanTwo,
        GreaterThanTwo
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        configuration.set("Search", args[2]);
        Job job = Job.getInstance(configuration, "Custom Word Count");
        job.setJarByClass(WordCountDriver.class);
        CustomWordCountInputFormat.setInputDirRecursive(job, true);
        CustomWordCountInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(CustomWordCountInputFormat.class);
        job.setMapperClass(WordCountMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        int status = job.waitForCompletion(true) ? 0 : 1;
        Counters counter = job.getCounters();
        System.out.println("Files w/ < 2 occurrences: " + counter.findCounter(WordCount.LessThanTwo).getValue());
        System.out.println("Files w/ = 2 occurrences: " + counter.findCounter(WordCount.EqualToTwo).getValue());
        System.out.println("Files w/ > 2 occurrences: " + counter.findCounter(WordCount.GreaterThanTwo).getValue());
        System.out.println("Uncompressed Bytes Output: " + counter.findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue());
        return status;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCountDriver(), args));
    }
}
