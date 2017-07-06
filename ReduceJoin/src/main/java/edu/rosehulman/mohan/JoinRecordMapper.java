package edu.rosehulman.mohan;

//cc JoinRecordMapper Mapper for tagging weather records for a reduce-side join
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//vv JoinRecordMapper
public class JoinRecordMapper
 extends Mapper<LongWritable, Text, TextPair, Text> {

@Override
protected void map(LongWritable key, Text value, Context context)
   throws IOException, InterruptedException {
		context.write(new TextPair(getStationID(value.toString()), "1"), value);
}

private String getStationID(String record){
	return record.substring(4, 10) + "-" + record.substring(10, 15);
}

}
