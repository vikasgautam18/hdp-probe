package com.gautam.mantra.hdfs;

import com.gautam.mantra.commons.ProbeFileSystem;
import com.gautam.mantra.commons.ProbeService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class ProbeHDFS implements ProbeFileSystem, ProbeService {

    public Boolean isReachable(Map<String, String> props) {

        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", props.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            FileSystem fs = FileSystem.get(URI.create(props.get("hdfsPath")), conf);
            return fs.exists(new Path("/user"));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Boolean createFolder(String folderName) {
        return null;
    }

    public Boolean createFile(String fileName) {
        return null;
    }

    public Boolean readFile(String fileName) {
        return null;
    }

    public Boolean deleteFile(String fileName) {
        return null;
    }

    public Boolean updatePermissions(String fileName) {
        return null;
    }
}
