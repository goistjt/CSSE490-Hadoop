package edu.rosehulman.goistjt;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WordCountRecordReader extends RecordReader<Text, IntWritable> {
    private FileSplit fileSplit;
    private Configuration conf;
    private Text key = new Text();
    private IntWritable value = new IntWritable();
    private boolean processed = false;


    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.conf = taskAttemptContext.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            Text temp = new Text();
            try {
                in = fs.open(file);
                IOUtils.readFully(in, contents, 0, contents.length);
                key.set(file.toString());
                temp.set(contents);
                value.set(getOccurrences(temp));
            } finally {
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }
        return false;
    }

    private int getOccurrences(Text temp) {
        String find = conf.get("Search");
        String text = temp.toString();
        return StringUtils.countMatches(text, find);
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public IntWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1f : 0f;
    }

    public void close() throws IOException {

    }
}
