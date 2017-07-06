package edu.rosehulman.goistjt;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Jeremiah Goist on 12/5/2016.
 */
public class FriendListReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> occurred = new HashSet<>();
        SortedSet<String> dups = new TreeSet<>();
        for (Text t : values) {
            String[] friends = t.toString().split(",");
            for (String friend : friends) {
                if (occurred.contains(friend)) {
                    dups.add(friend);
                } else {
                    occurred.add(friend);
                }
            }
        }
        List<String> inCommon = dups.stream().collect(Collectors.toList());
        StringBuilder builder = new StringBuilder();
        for (String s : inCommon) {
            builder.append(s);
            builder.append(",");
        }
        String toReturn = builder.toString();
        if (toReturn.length() > 0) {
            toReturn = toReturn.substring(0, toReturn.length() - 1);
        }
        context.write(key, new Text(toReturn));
    }
}
