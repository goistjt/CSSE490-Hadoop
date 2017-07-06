package edu.rosehulman.goistjt;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class Upper extends UDF {

    public Text evaluate(final Text t) {
        if (t == null) {
            return null;
        }
        return new Text(t.toString().toUpperCase());
    }
}
