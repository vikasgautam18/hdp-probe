package com.gautam.mantra;

import com.gautam.mantra.hdfs.ProbeHDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
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

        // is HDFS reachable
        if(!isReachable)
            logger.error("HDFS not reachable, skipping other HDFS Tests");
        else {
            logger.info("HDFS is reachable");

            Boolean isCreateFolderWorking = hdfs.createFolder(properties);
            // is folder creation possible
            if(isCreateFolderWorking){
                logger.info("HDFS test folder successfully created !");

                //is file creation possible
                Boolean isCreateFileWorking = hdfs.createFile(properties);
                if(isCreateFileWorking)
                    logger.info("HDFS test file successfully created !");
                else {
                    logger.error("HDFS test file cannot be created. exiting ...");
                    System.exit(1);
                }

                //is file copying possible
                if(hdfs.copyFileFromLocalFS(properties))
                    logger.info("HDFS copyFromLocal successful !");
                else {
                    logger.error("HDFS copy frol local failed exiting ...");
                    System.exit(1);
                }

                //is file read possible
                if(hdfs.readFile(properties))
                    logger.info("HDFS read file successful !");
                else {
                    logger.error("HDFS read file failed exiting ...");
                    System.exit(1);
                }

                hdfs.updatePermissions(properties);

                //is file deletion possible
                if(hdfs.deleteFile(properties))
                    logger.info("HDFS delete file successful !");
                else {
                    logger.error("HDFS delete file failed exiting ...");
                    System.exit(1);
                }

                // clean up
                logger.info("tests complete.. clean up in progress .. !");
                hdfs.cleanup(properties);

                logger.info("clean-up complete.. ");
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
