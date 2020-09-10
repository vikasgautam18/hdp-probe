package com.gautam.mantra.hdfs;

import com.gautam.mantra.commons.ProbeFileSystem;
import com.gautam.mantra.commons.ProbeService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Map;

public class ProbeHDFS implements ProbeFileSystem, ProbeService {

    public static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

    /**
     * This method verifies if HDFS is reachable
     * @param props cluster properties loaded from config file
     * @return True if HDFS is reachable False otherwise
     */
    public Boolean isReachable(Map<String, String> props) {
        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", props.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            FileSystem fs = FileSystem.get(URI.create(props.get("hdfsPath")), conf);

            return fs.exists(new Path("/"));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method verifies if HDFS folder creation from Java API is working as expected
     * @param props Cluster configuration as properties
     * @return True if creating folder works as expected, False otherwise
     */
    public Boolean createFolder(Map<String, String> props) {

        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", props.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            FileSystem fs = FileSystem.get(URI.create(props.get("hdfsPath")), conf);

            if(fs.exists(new Path (props.get("testHDFSFolder")))){
                logger.info("HDFS test folder already exists: Deleting it first");
                if (deleteFolder(props)){
                    logger.info("HDFS test folder successfully deleted !");
                }
                else {
                    logger.error("Test folder cannot be deleted, exiting... ");
                    return false;
                }
            }
            fs.mkdirs(new Path (props.get("testHDFSFolder")));
            return (fs.exists(new Path (props.get("testHDFSFolder"))));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method creates a new file in HDFS and writes some data into it
     * @param props Cluster configuration as properties
     * @return True if creating file works, false otherwise
     */
    @Override
    public Boolean createFile(Map<String, String> props) {

        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", props.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try{
            FileSystem fs = FileSystem.get(URI.create(props.get("hdfsPath")), conf);

            FSDataOutputStream outputStream = fs.create(new Path(props.get("testHDFSCreatePath")));
            outputStream.writeUTF(props.get("testText"));
            outputStream.close();

            return (fs.exists(new Path(props.get("testHDFSCreatePath"))));

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method copies a file from local FS to HDFS
     * @param props Cluster configuration as properties
     * @return True if the copying worked, false otherwise
     */
    public Boolean copyFileFromLocalFS(Map<String, String> props){

        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", props.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try{
            FileSystem fs = FileSystem.get(URI.create(props.get("hdfsPath")), conf);
            fs.copyFromLocalFile(new Path(props.get("testHDFSLocalFile")), new Path(props.get("testHDFSCopyPath")));

            return (fs.exists(new Path(props.get("testHDFSCopyPath"))));

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * this method reads a file from HDFS
     * @param props Cluster configuration as properties
     * @return True if the file is read correctly, false otherwise
     */
    @Override
    public Boolean readFile(Map<String, String> props) {

        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", props.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try{
            FileSystem fs = FileSystem.get(URI.create(props.get("hdfsPath")), conf);
            if(fs.exists(new Path(props.get("testHDFSCreatePath")))){

                FSDataInputStream inputStream = fs.open(new Path(props.get("testHDFSCreatePath")));
                return inputStream.readUTF().equals(props.get("testText"));
            }
            else {
                logger.error("Test file does not exist !");
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method deletes a file from HDFS
     * @param props Cluster configuration as properties
     * @return True of the deletion was successful, false otherwise
     */
    @Override
    public Boolean deleteFile(Map<String, String> props) {
        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", props.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            FileSystem fs = FileSystem.get(URI.create(props.get("hdfsPath")), conf);

            if(!fs.exists(new Path (props.get("testHDFSCopyPath")))){
                logger.error("Test file does not exist already");
            }
            else
                fs.delete(new Path (props.get("testHDFSCopyPath")), false);

            return (!fs.exists(new Path (props.get("testHDFSCopyPath"))));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method deletes a folder from HDFS
     * @param props Cluster configuration as properties
     * @return True if the deletion was successful, false otherwise
     */
    @Override
    public Boolean deleteFolder(Map<String, String> props) {
        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", props.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            FileSystem fs = FileSystem.get(URI.create(props.get("hdfsPath")), conf);

            if(!fs.exists(new Path (props.get("testHDFSFolder")))){
                logger.error("Test folder does not exist already, creating one... ");
            }
            else
                fs.delete(new Path (props.get("testHDFSFolder")), true);

            return (!fs.exists(new Path (props.get("testHDFSFolder"))));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method updates the permissions of a file/folder
     * @param props Cluster configuration as properties
     * @return True of the update was successful, false otherwise
     */
    @Override
    public Boolean updatePermissions(Map<String, String> props) {
        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", props.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            FileSystem fs = FileSystem.get(URI.create(props.get("hdfsPath")), conf);

            logger.info("current permission --> " + fs.getAclStatus(new Path(props.get("testHDFSCreatePath")))
                    .getPermission().toString());
            fs.setPermission(new Path(props.get("testHDFSCreatePath")), new FsPermission("0744"));
            logger.info("modified permission --> " + fs.getAclStatus(new Path(props.get("testHDFSCreatePath")))
                    .getPermission().toString());

            return fs.getAclStatus(new Path(props.get("testHDFSCreatePath")))
                    .getPermission().toString().equals("rwxr--r--");
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method cleans up all the test data created before
     * @param props Cluster configuration as properties
     */
    @Override
    public void cleanup(Map<String, String> props) {
        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", props.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            FileSystem fs = FileSystem.get(URI.create(props.get("hdfsPath")), conf);

            if(fs.exists(new Path (props.get("testHDFSFolder")))){
                fs.delete(new Path (props.get("testHDFSFolder")), true);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
