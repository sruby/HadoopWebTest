package io.github.sruby.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * HDFS test
 */
public class TestHDFS {

    private Configuration conf;
    private FileSystem fileSystem;

    @Before
    public void conn() throws IOException {
        conf = new Configuration();
        fileSystem = FileSystem.get(conf);
    }

    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/mytemp");
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
            boolean mkdirs = fileSystem.mkdirs(path);

        }
    }


    @After
    public void close() {

    }
}
