package com.gautam.mantra.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Random;

class ProbeHiveTest {
    static MiniDFSCluster.Builder builder;
    static Connection hiveConnection;
    static Statement stm;
    static MiniDFSCluster miniDFS;
    static MiniMRClientCluster miniMR;

    @BeforeAll
    static void setUp() throws IOException, SQLException {
        System.setProperty("hive.metastore.warehouse.dir", "/tmp");
        File baseDir = new File("./target/hdfs/" + ProbeHiveTest.class.getSimpleName()).getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);

        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        conf.set("dfs.namenode.acls.enabled", "true");
        builder = new MiniDFSCluster.Builder(conf);
        builder.nameNodePort(8020);
        miniDFS = builder.build();

        System.setProperty("hadoop.log.dir", "/tmp");
        int numTaskTrackers = 1;

        String identifier = ProbeHiveTest.class.getSimpleName() + "_"
                + new Random().nextInt(Integer.MAX_VALUE);

        miniMR = MiniMRClientClusterFactory.create(ProbeHiveTest.class,
                identifier, numTaskTrackers, new JobConf(miniDFS.getConfiguration(0)));
        System.setProperty("mapred.job.tracker", miniMR.getConfig().get("mapred.job.tracker"));

        try {
            String driverName = "org.apache.hive.jdbc.HiveDriver";
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        hiveConnection = DriverManager.getConnection("jdbc:hive2:///", "", "");
        stm = hiveConnection.createStatement();

    }

    @AfterAll
    static void tearDown() throws IOException, SQLException {
        cleanUp();
        miniMR.stop();
        miniDFS.shutdown(true, true);
        stm.close();
        hiveConnection.close();
    }

    private static void cleanUp() throws SQLException {
        stm.execute("set hive.support.concurrency = false");
        stm.execute("drop database if exists db_test");
    }

    @Test
    void test(){
        assert true;
    }

    @Test
    public void createDatabase() throws SQLException {

        stm.execute("set hive.support.concurrency = false");
        stm.execute("create database if not exists db_test");
        ResultSet res = stm.executeQuery("show databases");
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }
}