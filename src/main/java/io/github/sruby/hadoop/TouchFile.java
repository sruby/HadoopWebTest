package io.github.sruby.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;

public class TouchFile {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();

//	String hdfsPath = "hdfs://localhost:8020";
        FileSystem hdfs = FileSystem.get(configuration);

        String filePath = "/hdfstest/touchfile";

        FSDataOutputStream create = hdfs.create(new Path(filePath));

        System.out.println("Finish!");
    }
}
