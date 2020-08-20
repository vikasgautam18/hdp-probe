package com.gautam.mantra.hdfs;

import com.gautam.mantra.commons.ProbeFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

public class ProbeHDFS implements ProbeFileSystem {

    public Boolean isReachable(Map<String, Object> props) {
        Configuration conf= new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.addResource(props.get("coreSite").toString());

        System.out.println(conf.getRaw("fs.default.name"));

        try {
            FileSystem fs = FileSystem.get(conf);
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
