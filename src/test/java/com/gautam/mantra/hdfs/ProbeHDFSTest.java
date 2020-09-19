package com.gautam.mantra.hdfs;

import com.gautam.mantra.commons.Event;
import com.gautam.mantra.commons.Utilities;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.sql.Timestamp;
import java.util.Map;

class ProbeHDFSTest {
    static MiniDFSCluster hdfsCluster;
    static MiniDFSCluster.Builder builder;
    static final Configuration conf = new Configuration();
    static Map<String, String> properties;
    static ProbeHDFS hdfs;
    static final Utilities utilities = new Utilities();

    public static final Yaml yaml = new Yaml();
    public static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

    @BeforeAll
    static void setUp() {
        InputStream inputStream = ProbeHDFSTest.class.getClassLoader().getResourceAsStream("cluster-conf.yml");
        properties = yaml.load(inputStream);
        utilities.printProperties(properties);
        hdfs = new ProbeHDFS(properties);

        File baseDir = new File("./target/hdfs/" + ProbeHDFSTest.class.getSimpleName()).getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);

        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        conf.set("dfs.namenode.acls.enabled", "true");
        builder = new MiniDFSCluster.Builder(conf);
        builder.nameNodePort(8020);

        try {
            hdfsCluster = builder.build();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";
        logger.info("HDFS URI --> " + hdfsURI);
    }

    @AfterAll
    static void tearDown() {
        builder.checkExitOnShutdown(true);
        hdfs.cleanup();
        hdfsCluster.shutdown();
    }

    @Test
    void isReachable() {
        assert hdfs.isReachable();
    }

    @Test
    void createFolder() {
        assert hdfs.createFolder();
    }

    @Test
    void createFile() {
        assert hdfs.createFile();
    }

    @Test
    void copyFileFromLocalFS() {
        assert hdfs.copyFileFromLocalFS();
    }

    @Test
    void readFile() {
        hdfs.createFolder();
        hdfs.createFile();
        assert hdfs.readFile();
    }

    @Test
    void deleteFile() {
        assert hdfs.deleteFile();
    }

    @Test
    void deleteFolder() {
        assert hdfs.deleteFolder();
    }

    @Test
    void updatePermissions() {
        hdfs.createFolder();
        hdfs.createFile();
        assert hdfs.updatePermissions();
    }

    @Test
    void testEvents() {
        Event event = new Event("e1", new Timestamp(System.currentTimeMillis()));
        System.out.println(event.toJSON());
        JsonObject jobj = new Gson().fromJson(event.toJSON(), JsonObject.class);
        assert jobj.has("eventId");
        assert jobj.has("eventTs");
        assert jobj.get("eventId").getAsString().equals("e1");
    }
}