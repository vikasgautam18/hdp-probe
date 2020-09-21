package com.gautam.mantra.zookeeper;

import com.gautam.mantra.commons.ProbeService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ProbeZookeeper implements ProbeService {

    final ZKConnection zkConnection = new ZKConnection();
    private final Map<String, String> properties;
    private static ZooKeeper zooKeeper;
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

    public ProbeZookeeper(Map<String, String> properties) {
        this.properties = properties;
        initialize();
    }

    /**
     * Finds out if the ZK cluster is reachable
     * @return true if reachable, false otherwise
     */
    @Override
    public Boolean isReachable() {
        return zooKeeper.getState().isAlive();
    }

    /**
     * This method initializes the Zookeeper connection
     */
    private void initialize() {
        try {

            zooKeeper = zkConnection.connect(properties.get("zkHost"));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * This method closes the Zookeeper connection
     */
    public void closeConnection() {
        try {
            zkConnection.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method creates a new ZNode and adds some data to it
     * @param path the path of ZNode
     * @param data the data
     * @return True if the creation was successful, false otherwise
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean createZNodeData(String path, byte[] data) throws KeeperException, InterruptedException {
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        return (zooKeeper.exists(path, true) != null);
    }

    /**
     * This method verifies if the given ZNode exists
     * @param path the ZNode Path
     * @return true if the path exists, false otherwise
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean existsZNode(String path) throws KeeperException, InterruptedException {
        return zooKeeper.exists(path, true) != null;
    }

    /**
     * This method reads the data present at a given ZNode
     * @param path the ZNode path
     * @return true if the read was successful, false otherwise
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean getZNodeData(String path)
            throws KeeperException, InterruptedException {

        byte[] b;
        b = zooKeeper.getData(path, null, null);
        return !(new String(b, StandardCharsets.UTF_8).isEmpty());
    }

    /**
     * This method updates the data present at a given z-node with new content and increments the version
     * @param path the ZNode path
     * @param data the updated data
     * @return true if the update was successful, false otherwise
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean updateZNodeData(String path, byte[] data) throws KeeperException, InterruptedException {
        int version = zooKeeper.exists(path, true).getVersion();
        zooKeeper.setData(path, data, version);
        return (zooKeeper.exists(path, true).getVersion() > version);
    }

    /**
     * this method deletes the given ZNode
     * @param path the ZNode path
     * @return true if the delete was successful, false otherwise
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean deleteZNodeData(String path) throws KeeperException, InterruptedException {
        int version = zooKeeper.exists(path, true).getVersion();
        zooKeeper.delete(path, version);

        return zooKeeper.exists(path, true) == null;
    }
}