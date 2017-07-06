package edu.rosehulman.goistjt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedText implements WritableComparable<TaggedText> {
    private Text courseNum;
    private IntWritable tag;

    public TaggedText() {
        set(new Text(), new IntWritable());
    }

    public TaggedText(String course, int tag) {
        set(new Text(course), new IntWritable(tag));
    }

    public TaggedText(Text course, IntWritable tag) {
        set(course, tag);
    }

    public void set(Text courseNum, IntWritable tag) {
        this.courseNum = courseNum;
        this.tag = tag;
    }

    public Text getCourseNum() {
        return courseNum;
    }

    public IntWritable getTag() {
        return tag;
    }

    @Override
    public String toString() {
        return String.format("%s\t%s", courseNum.toString(), tag.toString());
    }

    public int compareTo(TaggedText o) {
        int cmp = courseNum.compareTo(o.courseNum);
        if (cmp != 0) {
            return cmp;
        }
        return tag.compareTo(o.tag);
    }

    @Override
    public int hashCode() {
        return courseNum.hashCode() * 162 + tag.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TaggedText && courseNum.equals(((TaggedText) obj).courseNum) && tag.equals(((TaggedText) obj).tag);
    }

    public void write(DataOutput dataOutput) throws IOException {
        courseNum.write(dataOutput);
        tag.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        courseNum.readFields(dataInput);
        tag.readFields(dataInput);
    }
}
