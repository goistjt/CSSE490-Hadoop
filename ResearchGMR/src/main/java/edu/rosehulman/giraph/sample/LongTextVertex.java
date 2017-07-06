package edu.rosehulman.giraph.sample;

import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import org.apache.giraph.hive.input.vertex.SimpleNoEdgesHiveToVertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class LongTextVertex extends SimpleNoEdgesHiveToVertex<LongWritable, Text> {

    public LongWritable getVertexId(HiveReadableRecord hiveReadableRecord) {
        return new LongWritable(hiveReadableRecord.getLong(1));
    }

    public Text getVertexValue(HiveReadableRecord hiveReadableRecord) {
        return new Text(hiveReadableRecord.getString(0));
    }

    public void checkInput(HiveInputDescription hiveInputDescription, HiveTableSchema hiveTableSchema) {
    }
}