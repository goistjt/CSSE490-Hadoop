package edu.rosehulman.goistjt;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;

public class TextPairComparator extends WritableComparator {
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

    public TextPairComparator() {
        super(TextPair.class);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

        try {
            System.out.println("Inside Custom Raw Comparator");
            int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
            int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
            int cmp = TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                    b2, s2 + firstL2, l2 - firstL2); // Sort by last name
            if (cmp != 0) {
                return 0 - cmp; //Flip the result
            }
            return 0 - TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2); // Flip result
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
