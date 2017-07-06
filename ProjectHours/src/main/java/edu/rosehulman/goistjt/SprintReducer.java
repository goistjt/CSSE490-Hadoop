package edu.rosehulman.goistjt;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SprintReducer extends Reducer<SprintTriple, DoubleWritable, SprintTriple, DoubleWritable> {
    @Override
    protected void reduce(SprintTriple key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        final double[] sum = {0};
        values.forEach(v -> sum[0] += v.get());
        context.write(key, new DoubleWritable(sum[0]));
    }
}
