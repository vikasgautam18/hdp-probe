package com.gautam.mantra.hdfs;

import com.gautam.mantra.commons.ProbeFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class ProbeHDFS implements ProbeFileSystem {

    public Boolean isReachable(Map<String, Object> props) {

        System.out.println("hdfsPath -> "+ props.get("hdfsPath").toString());
        System.out.println("coreSite -> "+ props.get("coreSite").toString());
        System.out.println("hdfsSite -> "+ props.get("hdfsSite").toString());

        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", props.get("hdfsPath").toString());
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        //conf.addResource(props.get("coreSite").toString());
        //conf.addResource(props.get("hdfsSite").toString());

        System.out.println(conf.getRaw("fs.default.name"));

        try {
            FileSystem fs = FileSystem.get(URI.create(props.get("hdfsPath").toString()), conf);
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
