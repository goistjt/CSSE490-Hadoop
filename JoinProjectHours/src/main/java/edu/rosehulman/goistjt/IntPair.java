package edu.rosehulman.goistjt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {
    private IntWritable first;
    private IntWritable second;

    public IntPair() {
        set(new IntWritable(), new IntWritable());
    }

    public IntPair(int first, int second) {
        set(new IntWritable(first), new IntWritable(second));
    }

    public IntPair(IntWritable first, IntWritable second) {
        set(first, second);
    }

    public void set(IntWritable first, IntWritable second) {
        this.first = first;
        this.second = second;
    }

    public IntWritable getFirst() {
        return first;
    }

    public IntWritable getSecond() {
        return second;
    }

    public int compareTo(IntPair o) {
        int cmp = first.compareTo(o.first);
        return cmp != 0 ? cmp : second.compareTo(o.second);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof IntPair && first.equals(((IntPair) obj).first) && second.equals(((IntPair) obj).second);
    }

    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }
}
