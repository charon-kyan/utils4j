package me.charon.utils4j.hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by a on 6/28/17.
 */
public class HDFSTextPath {

    protected static final Logger log = LoggerFactory.getLogger(HDFSTextPath.class);
    protected final String path;
    protected final HDFSFileSystem hfs;

    public HDFSTextPath(HDFSFileSystem hfs, String path) throws IOException {
        this.hfs = hfs;
        this.path = path;
    }

    public boolean exist() throws IOException {
        return hfs.exist(this.path);
    }
}
