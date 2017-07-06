package edu.rosehulman.goistjt;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Course Grade Reduce Join");
        job.setJarByClass(getClass());

        Path gradesInputPath = new Path(args[0]);
        Path courseInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        MultipleInputs.addInputPath(job, gradesInputPath, TextInputFormat.class, JoinGradesMapper.class);
        MultipleInputs.addInputPath(job, courseInputPath, TextInputFormat.class, JoinCoursesMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(FirstComparator.class);

        job.setMapOutputKeyClass(TaggedText.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Driver(), args));
    }

    public static class KeyPartitioner extends Partitioner<TaggedText, Text> {

        public int getPartition(TaggedText tagged, Text text, int i) {
            return (tagged.getCourseNum().hashCode() & Integer.MAX_VALUE) % i;
        }
    }
}
