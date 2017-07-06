package edu.rosehulman.goistjt;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Jeremiah Goist on 12/5/2016.
 */
public class FriendListMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException {
        String[] people = inValue.toString().split(",");
        for (int i = 1; i < people.length; i++) {
            Text outKey = makeKey(people, i);
            Text outValue = makeValues(people, i);
            context.write(outKey, outValue);
        }
    }

    private Text stringArrayToText(String[] temp) {
        StringBuilder builder = new StringBuilder();
        for (String s : temp) {
            builder.append(s);
            builder.append(",");
        }
        String out = builder.toString();
        out = out.substring(0, out.length()-1); //Trim the trailing comma
        return new Text(out);
    }

    private Text makeKey(String[] people, int second) {
        String[] temp = new String[] {people[0], people[second]};
        Arrays.sort(temp);
        return stringArrayToText(temp);
    }

    private Text makeValues(String[] people, int second) {
        List<String> peopleCopy = Arrays.asList(people);
        List<String> remaining = new LinkedList<>();
        for (String person : peopleCopy) {
            if(!person.equals(people[0]) && !person.equals(people[second])){
                remaining.add(person);
            }
        }
        String[] toReturn = remaining.toArray(new String[]{});
        return stringArrayToText(toReturn);
    }
}
