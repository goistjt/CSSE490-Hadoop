package edu.rosehulman.goistjt;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<TaggedText, Text, Text, Text> {

    @Override
    protected void reduce(TaggedText key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        String courseName = iterator.next().toString();
        String[] details;
        while (iterator.hasNext()) {
            details = iterator.next().toString().split(",");
            Text outKey = new Text(String.format("%s\t%s", details[0], key.getCourseNum().toString()));
            Text outVal = new Text(String.format("%s\t%s", courseName, details[1]));
            context.write(outKey, outVal);
        }
    }
}
