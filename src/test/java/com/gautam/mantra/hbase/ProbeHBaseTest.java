package com.gautam.mantra.hbase;

import com.gautam.mantra.commons.Utilities;
import com.gautam.mantra.zookeeper.ProbeZookeeper;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

class ProbeHBaseTest {

    private static HBaseTestingUtility utility;
    private static ProbeHBase hbase;
    private static Logger logger = LoggerFactory.getLogger(ProbeHBaseTest.class.getName());
    static Map<String, String> properties;
    public static final Yaml yaml = new Yaml();
    static final Utilities utilities = new Utilities();

    @BeforeAll
    static void setUp() {
        InputStream inputStream = ProbeZookeeper.class.getClassLoader().getResourceAsStream("cluster-conf.yml");
        properties = yaml.load(inputStream);
        utilities.printProperties(properties);

        logger.info("*** Setting up Hbase testing utility ***");
        utility = new HBaseTestingUtility();

        try {
            logger.info("*** Starting HBase Mini Cluster ***");
            //utility.startMiniZKCluster(1, 2181);
            utility.startMiniCluster();

            HBaseAdmin.available(utility.getConfiguration());
            logger.info("*** HBase Mini Cluster Successfully started ***");

            hbase = new ProbeHBase(utility.getConfiguration());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    static void tearDown() {
        logger.info("*** Shutting down Hbase Mini Cluster ***");
        try {
            utility.shutdownMiniCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void createDatabase() {
        assert hbase.createDatabase(properties.get("hbaseNS"));
    }

    @Test
    void deleteDatabase() {
        if(!hbase.existsNameSpace(properties.get("hbaseNS")))
            hbase.createDatabase(properties.get("hbaseNS"));
        assert hbase.deleteDatabase(properties.get("hbaseNS"));
    }

    @Test
    void createTable() {
    }

    @Test
    void readTable() {
    }

    @Test
    void deleteTable() {
    }

    @Test
    void isReachable() {
        assert hbase.isReachable(properties);
    }
}