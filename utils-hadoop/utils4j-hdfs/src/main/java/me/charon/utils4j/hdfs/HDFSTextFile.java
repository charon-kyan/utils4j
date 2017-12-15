package me.charon.utils4j.hdfs;

import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

/**
 * Created by a on 6/28/17.
 */
public class HDFSTextFile extends HDFSTextPath implements Iterable<String> {

    protected static final Logger log = LoggerFactory.getLogger(HDFSTextFile.class);
    protected Reader reader;

    public HDFSTextFile(HDFSFileSystem hfs, String path) throws IOException {
        super(hfs, path);
        if (!hfs.exist(path) || !hfs.isFile(path)) {
            throw new RuntimeException(String.format("%s is not exist or not a file", path));
        }
        this.reader = hfs.newFileReader(this.path);
    }

    @Override
    public Iterator<String> iterator() {
        return new LineIterator(reader);
    }
}
