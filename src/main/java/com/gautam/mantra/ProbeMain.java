package com.gautam.mantra;

import com.gautam.mantra.hdfs.ProbeHDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;


public class ProbeMain {
    public static Yaml yaml = new Yaml();

    public static void main(String[] args) {

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = loader.getResourceAsStream("cluster-conf.yml");

        Map<String, String> properties = yaml.load(inputStream);

        Logger logger = LoggerFactory.getLogger(ProbeMain.class.getName());

        printProperties(properties, logger);

        ProbeHDFS hdfs = new ProbeHDFS();
        Boolean isReachable = hdfs.isReachable(properties);
        logger.info("HDFS is reachable :: " + isReachable);
    }

    private static void printProperties(Map<String, String> properties, Logger logger) {
        for (String key: properties.keySet()) {
            logger.info(key + " --> " + properties.get(key));
        }
    }
}
