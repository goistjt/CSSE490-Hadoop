package edu.rosehulman.giraph.sample;

import org.apache.giraph.hive.HiveGiraphRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        if (strings.length != 3) {
            System.err.println("Usage: Driver: <db.vertex_table> <db.edge_table> <output_path>");
            System.exit(-1);
        }
        HiveGiraphRunner giraphRunner = new HiveGiraphRunner();
        giraphRunner.addVertexInput(LongTextVertex.class, strings[0], null);
        giraphRunner.addEdgeInput(LongNullEdge.class, strings[1], null);
        return giraphRunner.run(null);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Driver(), args));
    }


}
