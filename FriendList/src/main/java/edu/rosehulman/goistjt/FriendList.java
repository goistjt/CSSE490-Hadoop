package edu.rosehulman.goistjt;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Jeremiah Goist on 12/5/2016.
 */
public class FriendList {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: edu.rosehulman.goistjt.FriendList: <input_path> <output_path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(FriendList.class);
        job.setJobName("Friends List");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(FriendListMapper.class);
        job.setReducerClass(FriendListReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.out.println("about to submit job");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
