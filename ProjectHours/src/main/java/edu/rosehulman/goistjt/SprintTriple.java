package edu.rosehulman.goistjt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SprintTriple implements WritableComparable<SprintTriple> {
    private Text firstName;
    private Text lastName;
    private IntWritable sprintNum;

    public SprintTriple() {
        set(new Text(), new Text(), new IntWritable());
    }

    public SprintTriple(String firstName, String lastName, int sprintNum) {
        set(new Text(firstName), new Text(lastName), new IntWritable(sprintNum));
    }

    public SprintTriple(Text firstName, Text lastName, IntWritable sprintNum) {
        set(firstName, lastName, sprintNum);
    }

    public void set(Text firstName, Text lastName, IntWritable sprintNum) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.sprintNum = sprintNum;
    }

    @Override
    public int compareTo(SprintTriple o) {
        int cmp = this.lastName.compareTo(o.lastName);
        if (cmp != 0) {
            return cmp;
        }
        cmp = this.firstName.compareTo(o.firstName);
        if (cmp != 0) {
            return cmp;
        }
        return this.sprintNum.compareTo(o.sprintNum);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        firstName.write(out);
        lastName.write(out);
        sprintNum.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstName.readFields(in);
        lastName.readFields(in);
        sprintNum.readFields(in);
    }

    @Override
    public String toString() {
        return String.format("%s %s %d", firstName, lastName, sprintNum.get());
    }
}
