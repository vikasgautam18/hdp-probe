package com.gautam.mantra.hbase;

import com.gautam.mantra.commons.Utilities;
import com.gautam.mantra.zookeeper.ProbeZookeeper;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
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
    static Connection connection;

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

            hbase = new ProbeHBase();
            connection = hbase.getHBaseConnection(utility.getConfiguration());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    static void tearDown() {
        logger.info("*** Shutting down Hbase Mini Cluster ***");
        try {
            hbase.closeConnection(connection);
            utility.shutdownMiniCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void createDatabase() {
        assert hbase.createNameSpace(connection,properties.get("hbaseNS"));
        assert hbase.existsNameSpace(connection,properties.get("hbaseNS"));
    }

    @Test
    void deleteDatabase() {
        if(!hbase.existsNameSpace(connection,properties.get("hbaseNS")))
            hbase.createNameSpace(connection,properties.get("hbaseNS"));
        assert hbase.deleteNameSpace(connection, properties.get("hbaseNS"));
    }

    @Test
    void createTable() {
        if(!hbase.existsNameSpace(connection,properties.get("hbaseNS"))){
            hbase.createNameSpace(connection,properties.get("hbaseNS"));
        }
        assert hbase.createTable(connection, properties.get("hbaseNS"),
                properties.get("hbaseTable"), properties.get("hbaseCF"));
    }

    @Test
    void writeToAndReadFromTable() {
        if(!hbase.existsNameSpace(connection, properties.get("hbaseNS"))){
            hbase.createNameSpace(connection, properties.get("hbaseNS"));
            hbase.createTable(connection, properties.get("hbaseNS"),
                    properties.get("hbaseTable"), properties.get("hbaseCF"));
        }

        assert hbase.writeToTable(connection, properties.get("hbaseNS"),
                TableName.valueOf(properties.get("hbaseTable")), properties.get("hbaseCF").getBytes());
    }

    @Test
    void isReachable() {
        assert hbase.isReachable(connection, properties);
    }
}