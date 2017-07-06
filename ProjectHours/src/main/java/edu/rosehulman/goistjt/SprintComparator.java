package edu.rosehulman.goistjt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;

public class SprintComparator extends WritableComparator {
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    private static final IntWritable.Comparator INT_COMPARATOR = new IntWritable.Comparator();

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            System.out.println("Inside Custom Raw Comparator");
            int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
            int secondL1 = WritableUtils.decodeVIntSize(b1[s1 + firstL1]) + readVInt(b1, s1 + firstL1);
            int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
            int secondL2 = WritableUtils.decodeVIntSize(b2[s2 + firstL2]) + readVInt(b2, s2 + firstL2);

            int lastComp = TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                    b2, s2 + firstL2, l2 - firstL2);
            if (lastComp != 0) return lastComp;

            int firstComp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            if (firstComp != 0) return firstComp;

            return INT_COMPARATOR.compare(b1, s1 + secondL1, l1 - secondL1,
                    b2, s2 + secondL2, l2 - secondL2);

//            int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
//            if (cmp != 0) {
//                return cmp;
//            }
//            return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
//                    b2, s2 + firstL2, l2 - firstL2);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
