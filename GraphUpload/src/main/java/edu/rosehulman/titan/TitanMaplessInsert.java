package edu.rosehulman.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TitanMaplessInsert extends Configured implements Tool {
    private static TitanGraph titanGraph;
    private static TitanManagement management;

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: TitanMaplessInsert <hdfs_file>");
            System.exit(-1);
        }

        String inFile = args[0];

        initTitan();

        readFile(inFile);
    }

    private static void readFile(String inFile) throws IOException {
        Path path = new Path(inFile);
        FileSystem fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(fs.open(path))));
        String line;
        long counter = 0;
        while ((line = bufferedReader.readLine()) != null) {
            counter++;
            String[] split = line.split("\t");
            long[] ids = new long[2];
            for (int i = 0; i < 2; i++) {
                ids[i] = Long.valueOf(split[i]);
                TitanVertex v1;
                TitanVertex v2;

                v1 = titanGraph.addVertex("id", ids[0]);
                v2 = titanGraph.addVertex("id", ids[1]);
                v1.addEdge("link_to", v2);
                if (counter % 1000 == 0) {
                    management.commit();
                }
            }

        }

    }

    private static void initTitan() {
        Configuration conf = new BaseConfiguration();
        conf.setProperty("gremlin.graph", "com.thinkaurelius.titan.core.TitanFactory");
        conf.setProperty("storage.backend", "hbase");
        conf.setProperty("storage.hostname", "hadoop-07.csse.rose-hulman.edu,hadoop-36.csse.rose-hulman.edu,hadoop-37.csse.rose-hulman.edu");
        conf.setProperty("storage.batch-loading", true);
        conf.setProperty("storage.hbase.ext.zookeeper.znode.parent", "/hbase-unsecure");
        conf.setProperty("storage.hbase.ext.hbase.zookeeper.property.clientPort", 2181);
        conf.setProperty("cache.db-cache", true);
        conf.setProperty("cache.db-cache-clean-wait", 20);
        conf.setProperty("cache.db-cache-time", 180000);
        conf.setProperty("cache.db-cache-size", 0.5);
        conf.setProperty("storage.hbase.compat-class", "com.thinkaurelius.titan.diskstorage.hbase.HBaseCompat1_0");

        titanGraph = TitanFactory.open(conf);
        management = titanGraph.openManagement();
        management.commit();
    }

    public int run(String[] strings) throws Exception {
        return 0;
    }
}
