package com.gautam.mantra;

import com.gautam.mantra.commons.Utilities;
import com.gautam.mantra.hbase.ProbeHBase;
import com.gautam.mantra.hdfs.ProbeHDFS;
import com.gautam.mantra.zookeeper.ProbeZookeeper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;


public class ProbeMain {
    public static final Yaml yaml = new Yaml();
    public static final Logger logger = LoggerFactory.getLogger(ProbeMain.class.getName());

    public static void main(String[] args) {

        // Load Cluster properties and configurations
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = loader.getResourceAsStream("cluster-conf.yml");
        Utilities utilities = new Utilities();

        Map<String, String> properties = yaml.load(inputStream);
        // print all loaded properties to console
        utilities.printProperties(properties);

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
                    logger.error("HDFS copy from local failed exiting ...");
                    System.exit(1);
                }

                //is file read possible
                if(hdfs.readFile(properties))
                    logger.info("HDFS read file successful !");
                else {
                    logger.error("HDFS read file failed exiting ...");
                    System.exit(1);
                }

                // is file permission update possible
                if(hdfs.updatePermissions(properties)){
                    logger.info("HDFS file permission updates successful !");
                } else {
                    logger.error("HDFS ile permission updates failed exiting ...");
                    System.exit(1);
                }

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

                // zookeeper tests begin
                ProbeZookeeper zookeeper = new ProbeZookeeper(properties);
                if(zookeeper.isReachable(properties))
                    logger.info("Zookeeper is reachable...");
                else {
                    logger.error("Zookeeper is not reachable, exiting.. ");
                    System.exit(1);
                }

                try {
                    if(zookeeper.createZNodeData(properties.get("zkPath"), properties.get("zkData").getBytes())){
                        logger.info("ZNode creation successful");

                        if(zookeeper.getZNodeData(properties.get("zkPath"), true))
                            logger.info("ZNode data is readable");
                        else {
                            logger.error("ZNode data read failed, exiting...");
                            System.exit(1);
                        }

                        if(zookeeper.updateZNodeData(properties.get("zkPath"), properties.get("zkData").getBytes())){
                            logger.info("ZNode data update successful");
                        } else {
                            logger.error("ZNode data update failed, exiting...");
                            System.exit(1);
                        }

                        if(zookeeper.deleteZNodeData(properties.get("zkPath"))){
                            logger.info("ZNode data delete successful");
                        } else {
                            logger.error("ZNode data delete failed, exiting...");
                            System.exit(1);
                        }
                    }
                    else {
                        logger.error("ZNode creation failed, exiting...");
                        System.exit(1);
                    }
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }

                probeHBase(properties);

            }
            else
                logger.error("HDFS test folder cannot be created. exiting ...");
        }
    }

    private static void probeHBase(Map<String, String> properties) {

        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, properties.get("zkQuorum"));
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, properties.get("zkPort"));
        conf.set(HConstants.HBASE_DIR, properties.get("hbaseDataDir"));
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, properties.get("hbaseZnodeParent"));
        ProbeHBase hbase = new ProbeHBase(conf);

        logger.info("beginning HBase tests... ");

        if (!hbase.isReachable(conf)) throw new AssertionError("HBase is not reachable, exiting... ");
        logger.info("HBase service is reachable... ");

        if (!hbase.createNameSpace(properties.get("hbaseNS")))
            throw new AssertionError("HBase Namespace creation failed, exiting... ");
        logger.info("Namespace creation successful");

        if (!hbase.createTable(properties.get("hbaseNS"), properties.get("hbaseTable"),
                properties.get("hbaseCF"))) throw new AssertionError("Hbase table creation failed, exiting... ");
        logger.info("Table creation successful");

        if (!hbase.writeToTable(properties.get("hbaseNS"),
                TableName.valueOf(properties.get("hbaseTable")),
                properties.get("hbaseCF").getBytes()))
            throw new AssertionError("HBase write to table failed, exiting... ");
        logger.info("Write to table successful");

        if (!hbase.deleteNameSpace(properties.get("hbaseNS")))
            throw new AssertionError("HBase namespace deletion failed, exiting... ");
        logger.info("Namespace deletion successful... ");

        hbase.closeConnection();
        logger.info("HBase tests are successful.. ");
    }
}
