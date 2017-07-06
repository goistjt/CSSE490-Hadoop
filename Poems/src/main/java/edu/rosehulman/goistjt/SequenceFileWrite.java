package edu.rosehulman.goistjt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class SequenceFileWrite {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: Writer <directory> <filename>");
            System.exit(-1);
        }
        String inDirectory = args[0];
        String outFile = args[1];
        Configuration conf = new Configuration();

        Text key = new Text();
        Text value = new Text();
        Writer writer;
        try {
            writer = SequenceFile.createWriter(conf, Writer.file(new Path(outFile)),
                    Writer.keyClass(key.getClass()), Writer.valueClass(value.getClass()));
            parseDirectory(writer, inDirectory, key, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void parseDirectory(Writer writer, String inDirectory, Text key, Text value) throws IOException {
        Stream<java.nio.file.Path> paths = Files.walk(Paths.get(inDirectory));
        paths.forEach(path -> {
            if (Files.isRegularFile(path)) {
                try {
                    String contents = new String(Files.readAllBytes(path));
                    key.set(path.getFileName().toString());
                    value.set(contents);
                    writer.append(key, value);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
