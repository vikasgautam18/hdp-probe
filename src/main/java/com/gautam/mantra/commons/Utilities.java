package com.gautam.mantra.commons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.Map;

public class Utilities {
    public static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

    public boolean isPortAvailable(int port) {
        try (ServerSocket ignored = new ServerSocket(port); DatagramSocket ignored1 = new DatagramSocket(port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Simple print utility
     * @param properties cluster properties loaded from config file
     */
    public void printProperties(Map<String, String> properties) {
        logger.info("Begin printing properties ===========");
        for (String key: properties.keySet()) {
            logger.info(key + " --> " + properties.get(key));
        }
        logger.info("End printing properties ===========");
    }
}
