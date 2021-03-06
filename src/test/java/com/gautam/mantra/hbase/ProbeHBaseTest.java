package com.gautam.mantra.hbase;

import com.gautam.mantra.commons.Utilities;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Map;

class ProbeHBaseTest {

    private static HBaseTestingUtility utility;
    private static ProbeHBase hbase;
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    static Map<String, String> properties;
    public static final Yaml yaml = new Yaml();
    static final Utilities utilities = new Utilities();

    @BeforeAll
    static void setUp() {
        InputStream inputStream = ProbeHBaseTest.class.getClassLoader().getResourceAsStream("cluster-conf.yml");
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
            hbase.closeConnection();
            utility.shutdownMiniCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void createDatabase() {
        assert hbase.createNameSpace(properties.get("hbaseNS"));
        assert hbase.existsNameSpace(properties.get("hbaseNS"));
    }

    @Test
    void deleteDatabase() {
        if(!hbase.existsNameSpace(properties.get("hbaseNS")))
            hbase.createNameSpace(properties.get("hbaseNS"));
        assert hbase.deleteNameSpace(properties.get("hbaseNS"));
    }

    @Test
    void createTable() {
        if(!hbase.existsNameSpace(properties.get("hbaseNS"))){
            hbase.createNameSpace(properties.get("hbaseNS"));
        }
        assert hbase.createTable(properties.get("hbaseNS"),
                properties.get("hbaseTable"), properties.get("hbaseCF"));
    }

    @Test
    void writeToAndReadFromTable() {
        if(!hbase.existsNameSpace(properties.get("hbaseNS"))){
            hbase.createNameSpace(properties.get("hbaseNS"));
            hbase.createTable(properties.get("hbaseNS"),
                    properties.get("hbaseTable"), properties.get("hbaseCF"));
        }

        assert hbase.writeToTable(properties.get("hbaseNS"),
                TableName.valueOf(properties.get("hbaseTable")), properties.get("hbaseCF").getBytes());
    }

    @Test
    void isReachable() {
        assert hbase.isReachable(utility.getConfiguration());
    }
}