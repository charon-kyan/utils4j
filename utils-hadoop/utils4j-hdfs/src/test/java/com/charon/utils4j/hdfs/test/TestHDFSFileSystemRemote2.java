package com.charon.utils4j.hdfs.test;

import me.charon.utils4j.hdfs.HDFSFileSystem;
import me.charon.utils4j.hdfs.HDFSTextDir;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by a on 6/27/17.
 */
public class TestHDFSFileSystemRemote2 {

    private String hdfsURI = "hdfs://103-8-17-sh-100-h05.yidian.com:8020";

    private String testFolderPath() {
        // return hdfsURI + "/user/azkaban/camus/indata_str_documents_info/hourly/2017-07-06";
        return "/user/azkaban/camus/indata_str_documents_info/hourly/2017-07-06";
    }

    // private String basePath = "/Users/a/codes/devel/utils4j/utils-hadoop/utils4j-hdfs/src/test/resources/";
    private String basePath = "resources/";

    private List<String> siteFiles = Arrays.asList(
            basePath + "core-site.xml",
            basePath + "hdfs-site.xml"
    );

    @Before
    public void setup() throws IOException {

    }

    private void testReadLine(String name) throws IOException {
        String uri = testFolderPath() + "/" + name;
        HDFSFileSystem hfs = HDFSFileSystem.get(hdfsURI);
        Iterator<String> it = hfs.lineIterator(uri);
        while (it.hasNext()) {
            System.out.println("[line]" + it.next());
        }
    }


    @Ignore
    public void testReadPath1() throws IOException {
        testReadLine("00");
    }

    private void testListPath(String path) throws IOException {
        HDFSFileSystem hfs = HDFSFileSystem.get(hdfsURI);
        List<String> files = hfs.listAllFiles(path);
        files.stream().forEach( f -> System.out.println("[file] " + f));
    }

    @Test
    public void testListPath1() throws IOException {
        // testListPath(hdfsURI + "/tmp/yankunpeng/2017-06-20");
        testListPath(testFolderPath());
    }

    private void testReadLineOfPathLazy(String name) throws IOException {
        String uri = testFolderPath() + "/" + name;
        HDFSFileSystem hfs = HDFSFileSystem.get(hdfsURI);
        Iterator<String> it = new HDFSTextDir(hfs, uri).lazyIterator();
        while (it.hasNext()) {
            System.out.println("[line]" + it.next());
        }
    }

    @Ignore
    public void testReadLineOfPathLazy() throws IOException {
        testReadLineOfPathLazy("combine");
    }

    @After
    public void destroy() {
    }
}
