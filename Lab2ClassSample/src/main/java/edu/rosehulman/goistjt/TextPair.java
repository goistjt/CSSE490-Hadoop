package edu.rosehulman.goistjt;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {
    private Text firstName;
    private Text lastName;

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(Text first, Text last) {
        set(first, last);
    }

    public TextPair(String first, String last) {
        set(new Text(first), new Text(last));
    }

    public void set(Text first, Text last) {
        this.firstName = first;
        this.lastName = last;
    }

    public Text getFirstName() {
        return firstName;
    }

    public Text getLastName() {
        return lastName;
    }

    public int compareTo(TextPair o) {
        int comp = o.lastName.compareTo(lastName);
        if (comp != 0) {
            return comp;
        }
        return o.firstName.compareTo(firstName);
    }

    public void write(DataOutput dataOutput) throws IOException {
        firstName.write(dataOutput);
        lastName.write(dataOutput);
    }

    public void readFields(DataInput in) throws IOException {
        firstName.readFields(in);
        lastName.readFields(in);
    }

    @Override
    public String toString() {
        return lastName + " " + firstName;
    }
}
