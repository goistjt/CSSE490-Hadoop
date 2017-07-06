package edu.rosehulman.goistjt;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Strip extends UDF {
    public Text evaluate(Text t) {
        if (t == null) {
            return null;
        }
        return new Text(t.toString().replaceAll("[(),]", ""));
    }
}
