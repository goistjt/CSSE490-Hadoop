package edu.rosehulman.goistjt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SprintDriverComparator extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        // The configuration can be accessed by using the getConf() method associated with
        //the configurable interface which we implemented by extending the Configured class.
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Custom Comparator with custom writable");

        job.setJarByClass(SprintDriver.class);
        job.setSortComparatorClass(SprintComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(SprintTriple.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(SprintMapper.class);
        job.setReducerClass(SprintReducer.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {

        //Rather than calling the tool's run method directly, we call the Toolrunner.run static method
        //which creates a Configuration object and then automatically calls the run method associated with
        //Tool. It also uses the GenericConfigurationParser to pick up any special options specified using
        //the command line.
        int exitCode = ToolRunner.run(new SprintDriverComparator(), args);
        System.exit(exitCode);
    }
}
