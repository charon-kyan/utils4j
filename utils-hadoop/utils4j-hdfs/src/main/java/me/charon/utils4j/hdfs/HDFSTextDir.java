package me.charon.utils4j.hdfs;

import com.google.common.collect.Lists;
import org.apache.commons.collections.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by a on 6/28/17.
 */
public class HDFSTextDir extends HDFSTextPath implements Iterable<String> {

    protected static final Logger log = LoggerFactory.getLogger(HDFSTextDir.class);

    public HDFSTextDir(HDFSFileSystem hfs, String path) throws IOException {
        super(hfs, path);
        if (!hfs.exist(path) || !hfs.isDir(path)) {
            throw new RuntimeException(String.format("%s is not exist or not a path", path));
        }
    }

    public List<String> ListPaths() throws IOException {
        return hfs.listFiles(this.path);
    }

    public List<String> listDirs() throws IOException {
        return hfs.listDirs(this.path);
    }

    public List<String> listFiles() throws IOException {
        return hfs.listFiles(this.path);
    }

    public List<String> listAllFiles() throws IOException {
        return hfs.listAllFiles(path);
    }

    @Override
    public Iterator<String> iterator() {
        try {
            List<String> files = listAllFiles();
            if (files.size() > 100) {
                return lazyIterator(files);
            } else {
                return eagerIterator(files);
            }
        } catch (IOException e) {
            log.error("[iterator] could not list files for " + path, e);
            return IteratorUtils.emptyIterator();
        }
    }

    public Iterator<String> lazyIterator() throws IOException {
        return lazyIterator(listAllFiles());
    }

    private Iterator<String> lazyIterator(List<String> files) {
        log.debug("[iterator] iterator is lazy for {} files", files.size());
        return new Iterator<String>() {
            private Iterator<String> fileIt = files.iterator();
            private Iterator<String> lineIt = null;

            @Override
            public boolean hasNext() {
                try {
                    if (lineIt != null && lineIt.hasNext()) {
                        return true;
                    } else {
                        // if (lineIt == null || !lineIt.hasNex())
                        // check next non empty file iterator
                        while (fileIt.hasNext()) {
                            String f = fileIt.next();
                            lineIt = hfs.lineIteratorOfFile(f);
                            log.debug("[iterator] start to iterate for file {}", f);
                            if (lineIt.hasNext()) {
                                return true;
                            }
                        }
                        return false;
                    }
                } catch (IOException e) {
                    log.error("[iterator] exception when iterating", e);
                    return false;
                }
            }

            @Override
            public String next() {
                return hasNext() ? lineIt.next() : null;
            }
        };
    }

    public Iterator<String> eagerIterator() throws IOException {
        return eagerIterator(listAllFiles());
    }

    private Iterator<String> eagerIterator(List<String> files) {
        log.debug("[iterator] iterator is eager for {} files", files.size());
        try {
            List<Iterator<String>> its = Lists.newArrayList();
            for (String f : files) {
                its.add(hfs.lineIteratorOfFile(f));
            }
            return IteratorUtils.chainedIterator(its.toArray(new Iterator[its.size()]));
        } catch (IOException e) {
            log.error("[iterator] could not get iterator for " + path, e);
            return IteratorUtils.emptyIterator();
        }
    }
}
