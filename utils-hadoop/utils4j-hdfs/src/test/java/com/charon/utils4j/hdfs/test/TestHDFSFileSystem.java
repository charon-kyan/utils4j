package com.charon.utils4j.hdfs.test;

import me.charon.utils4j.hdfs.HDFSFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Created by a on 6/27/17.
 */
public class TestHDFSFileSystem {

    private MiniDFSCluster hdfsCluster;
    private FileSystem fs;
    private String hdfsURI;


    private String testFileURI() {
        return testFolderPath() + "/test1";
    }

    private String testFolderPath() {
        return hdfsURI + "/tmp/yankunpeng";
    }

    private String content() {
        return "line1\nline2\nline3\nline4";
    }

    @Before
    public void setup() throws IOException {

        File baseDir = new File("./test/hdfs/test1").getAbsoluteFile();
        Configuration conf = new HdfsConfiguration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        hdfsCluster.waitActive();

        fs = hdfsCluster.getFileSystem();
        hdfsURI = fs.getUri().toString();

        System.out.println("[url]" + hdfsURI);

        createFile(testFileURI());
    }

    private void createFile(String file) throws IOException {
        DFSTestUtil.createFile(fs, new Path(file), 1024, (short) 1, new Random().nextLong());
        DFSTestUtil.writeFile(fs, new Path(file), content());
    }

    @Ignore
    public void test1() throws IOException {
        HDFSFileSystem hfs = HDFSFileSystem.get(hdfsURI);
        List<String> paths = hfs.ListPaths(testFolderPath());
        paths.stream().forEach(System.out::println);
    }

    @Ignore
    public void test2() throws IOException {
        HDFSFileSystem hfs = HDFSFileSystem.get(hdfsURI);
        Iterator<String> it = hfs.lineIterator(testFileURI());
        while (it.hasNext()) {
            System.out.println("[line]" + it.next());
        }
    }

    @After
    public void destroy() {
        if (hdfsCluster != null)
            hdfsCluster.shutdown();
    }
}
