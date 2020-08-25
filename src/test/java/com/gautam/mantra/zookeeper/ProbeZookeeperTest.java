package com.gautam.mantra.zookeeper;

import com.gautam.mantra.commons.Utilities;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProbeZookeeperTest {
    static HBaseTestingUtility hbt = new HBaseTestingUtility();
    static MiniZooKeeperCluster cluster;
    static ProbeZookeeper probeZookeeper;
    static Map<String, String> properties;
    public static final Yaml yaml = new Yaml();
    static final Utilities utilities = new Utilities();
    public static final Logger logger = LoggerFactory.getLogger(ProbeZookeeper.class.getName());

    @BeforeAll
    static void setUp() {

        try {
            InputStream inputStream = ProbeZookeeper.class.getClassLoader().getResourceAsStream("cluster-conf.yml");
            properties = yaml.load(inputStream);
            utilities.printProperties(properties);
            //probeZookeeper = new ProbeZookeeper(properties);

            cluster = hbt.startMiniZKCluster(1,Integer.getInteger(properties.get("zkPort"), 2181));
            assertEquals(0, cluster.getBackupZooKeeperServerNum());

            probeZookeeper = new ProbeZookeeper(properties);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @AfterAll
    static void tearDown() {
        try {
            if(probeZookeeper.existsZNode(properties.get("zkPath")))
                probeZookeeper.deleteZNodeData(properties.get("zkPath"));
            probeZookeeper.closeConnection();
            hbt.shutdownMiniZKCluster();
        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    @Test
    void isReachable() {
        assert probeZookeeper.isReachable(properties);
    }

    @Test
    void testCreateZNodeData(){
        try {
            assert probeZookeeper.createZNodeData(properties.get("zkPath"), properties.get("zkData").getBytes());
            assert probeZookeeper.existsZNode(properties.get("zkPath"));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testGetZNodeData(){
        try {
            if(!probeZookeeper.existsZNode(properties.get("zkPath")))
                probeZookeeper.createZNodeData(properties.get("zkPath"), properties.get("zkData").getBytes());
            assert probeZookeeper.getZNodeData(properties.get("zkPath"), true);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testUpdateZNodeData() {
        try {
            if(!probeZookeeper.existsZNode(properties.get("zkPath")))
                probeZookeeper.createZNodeData(properties.get("zkPath"), properties.get("zkData").getBytes());
            assert probeZookeeper.updateZNodeData(properties.get("zkPath"), properties.get("zkDataUpdated").getBytes());
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testDeleteZNodeData() {
        try {
            if(!probeZookeeper.existsZNode(properties.get("zkPath")))
                probeZookeeper.createZNodeData(properties.get("zkPath"), properties.get("zkData").getBytes());
            assert probeZookeeper.deleteZNodeData(properties.get("zkPath"));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}