package edu.rosehulman.giraph.sample;

import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import org.apache.giraph.hive.input.edge.SimpleHiveToEdge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class LongNullEdge extends SimpleHiveToEdge<LongWritable, NullWritable> {

    public LongWritable getSourceVertexId(HiveReadableRecord hiveReadableRecord) {
        return new LongWritable(hiveReadableRecord.getLong(0));
    }

    public LongWritable getTargetVertexId(HiveReadableRecord hiveReadableRecord) {
        return new LongWritable(hiveReadableRecord.getLong(1));
    }

    public NullWritable getEdgeValue(HiveReadableRecord hiveReadableRecord) {
        return null;
    }

    public void checkInput(HiveInputDescription hiveInputDescription, HiveTableSchema hiveTableSchema) {
    }
}