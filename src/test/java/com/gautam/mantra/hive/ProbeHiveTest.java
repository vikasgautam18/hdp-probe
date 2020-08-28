package com.gautam.mantra.hive;

import com.gautam.mantra.commons.Utilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

class ProbeHiveTest {
    static MiniDFSCluster.Builder builder;
    static Connection hiveConnection;
    static Statement stm;
    static MiniDFSCluster miniDFS;
    static MiniMRClientCluster miniMR;
    static ProbeHive hive;
    static Map<String, String> properties;
    public static final Yaml yaml = new Yaml();
    static final Utilities utilities = new Utilities();

    @BeforeAll
    static void setUp() throws IOException, SQLException {
        InputStream inputStream = ProbeHiveTest.class.getClassLoader().getResourceAsStream("cluster-conf.yml");
        properties = yaml.load(inputStream);
        utilities.printProperties(properties);

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

        hiveConnection = DriverManager.getConnection(properties.get("hiveJDBCURL"), "", "");
        stm = hiveConnection.createStatement();
        hive = new ProbeHive(properties);
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
        stm.execute("drop database if exists " + properties.get("hiveDatabase") + " cascade");
    }

    @Test
    public void testCreationAndDeletion() throws SQLException {
        cleanUp();
        assert hive.createDatabase(properties.get("hiveDatabase"));
        assert hive.createTable(properties.get("hiveTableCreateStmt"),
                properties.get("hiveDatabase"), properties.get("hiveTable"));

        hive.writeToTable(properties);

        ResultSet rs = stm.executeQuery("select * from " +
                        String.join(".", properties.get("hiveDatabase"), properties.get("hiveTable")));
        Map<Integer, String> resultMap = new HashMap<>();
        while(rs.next()){
            resultMap.put(rs.getInt("key"), rs.getString("value"));
        }

        assert !resultMap.isEmpty();
        assert resultMap.size() == 2;
        assert resultMap.get(1).equals("one");
        assert resultMap.get(2).equals("two");

        assert hive.dropTable(properties.get("hiveDatabase"), properties.get("hiveTable"));
        assert hive.dropDatabase(properties.get("hiveDatabase"), false);
    }
}