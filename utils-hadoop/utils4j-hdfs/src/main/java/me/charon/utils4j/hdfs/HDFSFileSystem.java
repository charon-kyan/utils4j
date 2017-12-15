package me.charon.utils4j.hdfs;

import com.google.common.collect.Lists;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.*;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by a on 6/27/17.
 */
public class HDFSFileSystem {

    protected final FileSystem fs;
    protected final Configuration conf;

    static private final String ENV_HADOOP_CONF = "/etc/hadoop/conf/";

    HDFSFileSystem(FileSystem fs, Configuration conf) {
        this.fs = fs;
        this.conf = conf;
    }

    public static HDFSFileSystem get() throws IOException {
        Configuration conf = new Configuration();

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        FileSystem fs = FileSystem.get(conf);
        if (fs == null) {
            throw new IOException("failed to get file system");
        }
        return new HDFSFileSystem(fs, conf);
    }

    public static HDFSFileSystem get(List<String> configPaths) throws IOException {
        Configuration conf = new Configuration();
        for (String p: configPaths) {
            File file = new File(p);
            if (!file.exists()) {
                throw new IOException("the site file " + p + " does not exist");
            }
        }

        configPaths.forEach(p -> conf.addResource(p));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        FileSystem fs = FileSystem.get(conf);
        if (fs == null) {
            throw new IOException("failed to get file system");
        }

        return new HDFSFileSystem(fs, conf);
    }

    public static HDFSFileSystem get(String uri) throws IOException {
        Configuration conf = new Configuration();
        conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        if (fs == null) {
            throw new IOException("failed to get file system");
        }

        return new HDFSFileSystem(fs, conf);
    }

    public FileSystem getFileSystem() {
        return fs;
    }

    public Configuration getConfiguration() {
        return conf;
    }

    public boolean isFile(String file) throws IOException {
        return fs.getFileStatus(new Path(file)).isFile();
    }

    public boolean isDir(String file) throws IOException {
        return fs.getFileStatus(new Path(file)).isDirectory();
    }

    public boolean exist(String file) throws IOException {
        return fs.exists(new Path(file));
    }

    public List<String> ListPaths(String path) throws IOException {
        if (exist(path)) {
            return Arrays.asList(fs.listStatus(new Path(path))).stream()
                    .map(FileStatus::getPath)
                    .map(p -> p.toUri().toString())
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public List<String> listDirs(String path) throws IOException {
        if (exist(path)) {
            return Arrays.asList(fs.listStatus(new Path(path))).stream()
                    .filter(FileStatus::isDirectory)
                    .map(FileStatus::getPath)
                    .map(p -> p.toUri().toString())
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public List<String> listFiles(String path) throws IOException {
        if (exist(path)) {
            return Arrays.asList(fs.listStatus(new Path(path))).stream()
                    .filter(FileStatus::isFile)
                    .map(FileStatus::getPath)
                    .map(p -> p.toUri().toString())
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public List<String> listAllFiles(String path) throws IOException {
        if (exist(path)) {
            List<String> all = Lists.newArrayList();
            all.addAll(listFiles(path));
            for (String dir : listDirs(path)) {
                all.addAll(listAllFiles(dir));
            }
            return all;
        }
        return Collections.emptyList();
    }

    Reader newTextFileReader(Path path) throws IOException {
        return new BufferedReader(new InputStreamReader(fs.open(path)));
    }

    Reader newCodecFileReader(Path path, CompressionCodec codec) throws IOException {
        return new BufferedReader(new InputStreamReader(codec.createInputStream(fs.open(path))));
    }

    public Reader newFileReader(String file) throws IOException {
        Path path = new Path(file);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(path);
        if (codec == null) {
            return newTextFileReader(path);
        } else {
            return newCodecFileReader(path, codec);
        }
    }

    Iterator<String> lineIteratorOfFile(String path) throws IOException {
        return new HDFSTextFile(this, path).iterator();
    }

    Iterator<String> lineIteratorOfDir(String path) throws IOException {
        return new HDFSTextDir(this, path).iterator();
    }

    public Iterator<String> lineIterator(String path) throws IOException {
        if (exist(path)) {
            if (isDir(path)) {
                return lineIteratorOfDir(path);
            } else if (isFile(path)) {
                return lineIteratorOfFile(path);
            }
        }
        return IteratorUtils.emptyIterator();
    }
}
