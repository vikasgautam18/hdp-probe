package com.gautam.mantra.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars;


class ProbeHiveTest {
    static MiniDFSCluster.Builder builder;

    @BeforeEach
    void setUp() {

    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void test(){
        assert true;
    }

    @Test
    public void testHiveMiniDFSClusterIntegration() throws IOException, SQLException {
        System.setProperty(ConfVars.METASTOREWAREHOUSE.toString(), "/tmp");
        File baseDir = new File("./target/hdfs/" + ProbeHiveTest.class.getSimpleName()).getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);

        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        conf.set("dfs.namenode.acls.enabled", "true");
        builder = new MiniDFSCluster.Builder(conf);
        builder.nameNodePort(8020);
        /* Build MiniDFSCluster */
        MiniDFSCluster miniDFS = builder.build();

        Configuration conf1= new Configuration();
        conf1.set("fs.defaultFS", "hdfs://localhost:8020");
        conf1.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf1.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:8020"), conf1);

            if(!fs.exists(new Path("/tmp/hive"))){
                fs.mkdirs(new Path ("/tmp/hive"));
                fs.setPermission(new Path("/tmp/hive"), new FsPermission("0777"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        /* Build MiniMR Cluster */
        System.setProperty("hadoop.log.dir", "/tmp");
        int numTaskTrackers = 1;
        int numTaskTrackerDirectories = 1;
        String[] racks = null;
        String[] hosts = null;
        MiniMRCluster miniMR = new MiniMRCluster(numTaskTrackers, miniDFS.getFileSystem().getUri().toString(),
                numTaskTrackerDirectories, racks, hosts, new JobConf(conf));

        System.setProperty("mapred.job.tracker", miniMR.createJobConf(
                new JobConf(conf)).get("mapred.job.tracker"));

        try {
            String driverName = "org.apache.hive.jdbc.HiveDriver";
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection hiveConnection = DriverManager.getConnection(
                "jdbc:hive2:///", "", "");
        Statement stm = hiveConnection.createStatement();

        // now create test tables and query them
        stm.execute("set hive.support.concurrency = false");
        stm.execute("drop table if exists test");
        stm.execute("create table if not exists test(a int, b int) row format delimited fields terminated by ' '");
        stm.execute("create table dual as select 1 as one from test");
        stm.execute("insert into table test select stack(1,4,5) AS (a,b) from dual");
        stm.execute("select * from test");
    }
}