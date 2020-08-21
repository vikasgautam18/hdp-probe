package com.gautam.mantra;

import com.gautam.mantra.hdfs.ProbeHDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;


public class ProbeMain {
    public static Yaml yaml = new Yaml();
    public static Logger logger = LoggerFactory.getLogger(ProbeMain.class.getName());

    public static void main(String[] args) {

        // Load Cluster properties and configurations
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = loader.getResourceAsStream("cluster-conf.yml");

        Map<String, String> properties = yaml.load(inputStream);
        // print all loaded properties to console
        printProperties(properties);

        // begin probe - HDFS first
        ProbeHDFS hdfs = new ProbeHDFS();
        Boolean isReachable = hdfs.isReachable(properties);

        if(!isReachable)
            logger.error("HDFS not reachable, skipping other HDFS Tests");
        else {
            logger.info("HDFS is reachable");

            Boolean isCreateFolderWorking = hdfs.createFolder(properties);
            if(isCreateFolderWorking){
                logger.info("HDFS test folder successfully created !");

                Boolean isCreateFileWorking = hdfs.createFile(properties);
                if(isCreateFileWorking)
                    logger.info("HDFS test file successfully created !");
                else
                    logger.error("HDFS test file cannot be created. exiting ...");
            }
            else
                logger.error("HDFS test folder cannot be created. exiting ...");
        }
    }

    /**
     *
     * @param properties cluster properties loaded from config file
     */
    private static void printProperties(Map<String, String> properties) {
        for (String key: properties.keySet()) {
            logger.info(key + " --> " + properties.get(key));
        }
    }
}
