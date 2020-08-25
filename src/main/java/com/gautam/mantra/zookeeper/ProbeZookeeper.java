package com.gautam.mantra.zookeeper;

import com.gautam.mantra.commons.ProbeService;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ProbeZookeeper implements ProbeService {

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
            ZKConnection zkConnection = new ZKConnection();
            zooKeeper = zkConnection.connect(properties.get("zkHost"));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}