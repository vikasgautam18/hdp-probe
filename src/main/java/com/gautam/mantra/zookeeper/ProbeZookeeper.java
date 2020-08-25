package com.gautam.mantra.zookeeper;

import com.gautam.mantra.commons.ProbeService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ProbeZookeeper implements ProbeService {

    ZKConnection zkConnection = new ZKConnection();
    private static ZooKeeper zooKeeper;
    private static final Logger logger = LoggerFactory.getLogger(ProbeZookeeper.class);

    public ProbeZookeeper(Map<String, String> properties) {
        initialize(properties);
    }

    @Override
    public Boolean isReachable(Map<String, String> properties) {
        return zooKeeper.getState().isAlive();
    }

    private void initialize(Map<String, String> properties) {
        try {

            zooKeeper = zkConnection.connect(properties.get("zkHost"));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void closeConnection() {
        try {
            zkConnection.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean createZNodeData(String path, byte[] data) throws KeeperException, InterruptedException {
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        return (zooKeeper.exists(path, true) != null);
    }

    public boolean getZNodeData(String path, boolean watchFlag)
            throws KeeperException, InterruptedException {

        byte[] b;
        b = zooKeeper.getData(path, null, null);
        return !(new String(b, StandardCharsets.UTF_8).isEmpty());
    }

    public boolean updateZNodeData(String path, byte[] data) throws KeeperException, InterruptedException {
        int version = zooKeeper.exists(path, true).getVersion();
        zooKeeper.setData(path, data, version);
        return (zooKeeper.exists(path, true).getVersion() > version);
    }

    public boolean deleteZNodeData(String path) throws KeeperException, InterruptedException {
        int version = zooKeeper.exists(path, true).getVersion();
        zooKeeper.delete(path, version);

        return zooKeeper.exists(path, true) == null;
    }
}