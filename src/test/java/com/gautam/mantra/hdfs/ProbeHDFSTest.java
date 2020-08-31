package com.gautam.mantra.hdfs;

import com.gautam.mantra.commons.Utilities;
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
import java.util.Map;

class ProbeHDFSTest {
    static MiniDFSCluster hdfsCluster;
    static MiniDFSCluster.Builder builder;
    static final Configuration conf = new Configuration();
    static Map<String, String> properties;
    static final ProbeHDFS hdfs = new ProbeHDFS();
    static final Utilities utilities = new Utilities();

    public static final Yaml yaml = new Yaml();
    public static final Logger logger = LoggerFactory.getLogger(ProbeHDFSTest.class.getName());

    @BeforeAll
    static void setUp() {

        InputStream inputStream = ProbeHDFSTest.class.getClassLoader().getResourceAsStream("cluster-conf.yml");
        properties = yaml.load(inputStream);
        utilities.printProperties(properties);

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
        hdfs.cleanup(properties);
        hdfsCluster.shutdown();
    }

    @Test
    void isReachable() {
        assert hdfs.isReachable(properties);
    }

    @Test
    void createFolder() {
        assert hdfs.createFolder(properties);
    }

    @Test
    void createFile() {
        assert hdfs.createFile(properties);
    }

    @Test
    void copyFileFromLocalFS() {
        assert hdfs.copyFileFromLocalFS(properties);
    }

    @Test
    void readFile() {
        hdfs.createFolder(properties);
        hdfs.createFile(properties);
        assert hdfs.readFile(properties);
    }

    @Test
    void deleteFile() {
        assert hdfs.deleteFile(properties);
    }

    @Test
    void deleteFolder() {
        assert hdfs.deleteFolder(properties);
    }

    @Test
    void updatePermissions() {
        hdfs.createFolder(properties);
        hdfs.createFile(properties);
        assert hdfs.updatePermissions(properties);
    }
}