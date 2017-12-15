package com.charon.utils4j.hdfs.test;

import me.charon.utils4j.hdfs.HDFSFileSystem;
import me.charon.utils4j.hdfs.HDFSTextDir;
import me.charon.utils4j.hdfs.HDFSTextPath;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by a on 6/27/17.
 */
public class TestHDFSFileSystemRemote {

    private String hdfsURI = "hdfs://10.103.17.229:8020";

    private String testFolderPath() {
        return hdfsURI + "/tmp/yankunpeng";
    }

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
    public void testReadFile1() throws IOException {
        testReadLine("opp_documents_test1");
    }

    @Ignore
    public void testReadFile2() throws IOException {
        testReadLine("opp_documents_test2.gz");
    }

    @Ignore
    public void testReadPath1() throws IOException {
        testReadLine("combine");
    }

    private void testListPath(String path) throws IOException {
        HDFSFileSystem hfs = HDFSFileSystem.get(hdfsURI);
        List<String> files = hfs.listAllFiles(path);
        files.stream().forEach( f -> System.out.println("[file] " + f));
    }

    @Ignore
    public void testListPath1() throws IOException {
        // testListPath(hdfsURI + "/tmp/yankunpeng/2017-06-20");
        testListPath("/tmp/yankunpeng/2017-06-20");
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
