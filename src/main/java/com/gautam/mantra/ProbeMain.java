package com.gautam.mantra;

import com.gautam.mantra.commons.ClusterStatusResult;
import com.gautam.mantra.commons.Utilities;
import com.gautam.mantra.hbase.ProbeHBase;
import com.gautam.mantra.hdfs.ProbeHDFS;
import com.gautam.mantra.hive.ProbeHive;
import com.gautam.mantra.kafka.ProbeKafka;
import com.gautam.mantra.oozie.ProbeOozie;
import com.gautam.mantra.spark.ProbeSpark;
import com.gautam.mantra.spark.extras.RunExtras;
import com.gautam.mantra.spark.extras.ShopAnalysisPart1;
import com.gautam.mantra.zeppelin.ProbeZeppelin;
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
import java.lang.invoke.MethodHandles;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ProbeMain {
    public static final Yaml yaml = new Yaml();
    public static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    public static final ClusterStatusResult result = new ClusterStatusResult();

    public static void main(String[] args) {

        // Load Cluster properties and configurations
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = loader.getResourceAsStream("cluster-conf.yml");
        Utilities utilities = new Utilities();

        Map<String, String> properties = yaml.load(inputStream);
        // print all loaded properties to console
        utilities.printProperties(properties);

        // HDFS tests
        probeHDFS(properties);

        // ZOOKEEPER tests
        zookeeperProbe(properties);

        // HBase tests
        probeHBase(properties);

        // Hive tests
        probeHive(properties);

        // Oozie tests
        probeOozie(properties);

        //probeKafka
        probeKafka(properties);

        //probeZeppelin

        probeZeppelin(properties);

        // probeSpark
        probeSpark(properties);

        if(Boolean.parseBoolean(properties.get("execute.extras.flag"))){
            probeExtras(properties);
        }

        System.out.println(result.toString());
    }

    private static void probeOozie(Map<String, String> properties) {
        ProbeOozie oozie = new ProbeOozie(properties);
        result.setOozieStatus(true);

        if(oozie.isReachable()){
            logger.info("Oozie is reachable, proceeding with tests");
            if(oozie.invokeWorkflow()) {
                logger.info("Oozie tests successful.. ");
            } else {
                logger.error("Oozie tests failed..");
                result.setOozieStatus(false);
            }
        } else {
            logger.error("Oozie is not reachable");
            result.setOozieStatus(false);
        }
    }

    private static void probeExtras(Map<String, String> properties) {
        RunExtras extras = new RunExtras(properties);
        if(extras.submitBixiBusiestStationJob()) {
            logger.info(String.format("Spark job '%s' successfully completed",
                    properties.get("bixi.busiest.station")));
        } else {
            logger.error(String.format("An issue occurred while executing spark job '%s' ",
                    properties.get("bixi.busiest.station")));
        }

        if(extras.submitTop10LongestTripsJob()) {
            logger.info(String.format("Spark job '%s' successfully completed",
                    properties.get("bixi.longest.trips")));
        } else {
            logger.error(String.format("An issue occurred while executing spark job '%s' ",
                    properties.get("bixi.longest.trips")));
        }

        if(extras.submitMonthlyAverageJob()) {
            logger.info(String.format("Spark job '%s' successfully completed",
                    properties.get("bixi.monthly.averages")));
        } else {
            logger.error(String.format("An issue occurred while executing spark job '%s' ",
                    properties.get("bixi.monthly.averages")));
        }

        if(extras.submitAverageTripsPerDayOfMonthJob()) {
            logger.info(String.format("Spark job '%s' successfully completed",
                    properties.get("bixi.day0fmonth.averages")));
        } else {
            logger.error(String.format("An issue occurred while executing spark job '%s' ",
                    properties.get("bixi.day0fmonth.averages")));
        }

        if(extras.submitGenericJob(properties.get("shop.data.analysis1.application"), ShopAnalysisPart1.class.getName())) {
            logger.info(String.format("Spark job '%s' successfully completed",
                    properties.get("shop.data.analysis1.application")));
        } else {
            logger.error(String.format("An issue occurred while executing spark job '%s' ",
                    properties.get("shop.data.analysis1.application")));
        }

    }

    private static void probeZeppelin(Map<String, String> properties) {
        result.setZeppelinStatus(true);
        ProbeZeppelin zeppelin = new ProbeZeppelin(properties);
        if(zeppelin.probeZeppelinNote()){
            logger.info("Zeppelin tests successful !!");
        } else {
            logger.error("Zeppelin tests have failed... ");
            result.setZeppelinStatus(false);
        }
    }

    /**
     * Verifies Kafka service and basic functionality
     * @param properties The cluster properties
     */
    private static void probeKafka(Map<String, String> properties) {
        result.setKafkaStatus(true);
        ProbeKafka kafka = new ProbeKafka(properties);
        if(kafka.isReachable()){
            logger.info("Kafka service is reachable");
            if(kafka.createTopic(properties.get("kafka.probe.topic"))){
                logger.info("Topic creation successful... ");
                kafka.describeTopic(properties.get("kafka.probe.topic"));

                List<String> dataset = kafka.generateEventDataset();
                if(kafka.publishToTopic(properties.get("kafka.probe.topic"), dataset)){
                    logger.info("data published to Kafka successfully.. ");
                } else {
                    logger.info("Error publishing data to Kafka.. ");
                    result.setKafkaStatus(false);
                }

                if(kafka.deleteTopic(properties.get("kafka.probe.topic"))){
                    logger.info("Topic deletion successful..");
                } else {
                    logger.error("Topic deletion fails ... ");
                    result.setKafkaStatus(false);
                }
            } else {
                logger.error("Topic creation failed ... ");
                result.setKafkaStatus(false);
            }
        } else {
            logger.error("Kafka service is not reachable ... ");
            result.setKafkaStatus(false);
        }

        logger.info("Kafka tests successful... !!");
    }

    /**
     * Verifies spark functionality and basic functionalities including:
     * Spark - HDFS integration
     * SPark - SQL functionality
     * Spark - Hive integration
     * Spark - HBase integration
     * @param properties The cluster properties
     */
    private static void probeSpark(Map<String, String> properties) {
        ProbeSpark spark = new ProbeSpark();
        result.setSparkStatus(true);

        if(!spark.submitPiExampleJob(properties)){
            logger.error("Spark job submission failed ...");
            result.setSparkStatus(false);
        }
        logger.info("Spark pi example job submission is successful.. ");

        if(!spark.submitHDFSJob(properties)){
            logger.error("Spark job submission failed ...");
            result.setSparkStatus(false);
        }
        logger.info("Spark HDFS tests are successful.. ");

        if(!spark.submitSparkHiveJob(properties)){
            logger.error("Spark job submission failed ...");
            result.setSparkStatus(false);
        }
        logger.info("Spark Hive tests are successful.. ");

        if(!spark.submitSparkSQLJob(properties)){
            logger.error("Spark job submission failed ...");
            result.setSparkStatus(false);
        }
        logger.info("Spark SQL tests are successful.. ");

        if(!spark.submitSparkHBaseJob(properties)){
            logger.error("Spark job submission failed, exiting ...");
            result.setSparkStatus(false);
        }
        logger.info("Spark tests are successful.. ");
    }

    /**
     * Set of HDFS tests
     * @param properties The cluster properties
     */
    private static void probeHDFS(Map<String, String> properties) {
        // begin with Status true
        result.setHdfsStatus(true);

        ProbeHDFS hdfs = new ProbeHDFS(properties);
        Boolean isReachable = hdfs.isReachable();

        // is HDFS reachable
        if(!isReachable){
            logger.error("HDFS not reachable, skipping other HDFS Tests");
            result.setHdfsStatus(false);
        } else {
            logger.info("HDFS is reachable");

            Boolean isCreateFolderWorking = hdfs.createFolder();
            // is folder creation possible
            if(isCreateFolderWorking){
                logger.info("HDFS test folder successfully created !");

                //is file creation possible
                Boolean isCreateFileWorking = hdfs.createFile();
                if(isCreateFileWorking)
                    logger.info("HDFS test file successfully created !");
                else {
                    logger.error("HDFS test file cannot be created ...");
                    result.setHdfsStatus(false);
                }

                //is file copying possible
                if(hdfs.copyFileFromLocalFS())
                    logger.info("HDFS copyFromLocal successful !");
                else {
                    logger.error("HDFS copy from local failed ...");
                    result.setHdfsStatus(false);
                }

                //is file read possible
                if(hdfs.readFile())
                    logger.info("HDFS read file successful !");
                else {
                    logger.error("HDFS read file failed ...");
                    result.setHdfsStatus(false);
                }

                // is file permission update possible
                if(hdfs.updatePermissions()){
                    logger.info("HDFS file permission updates successful !");
                } else {
                    logger.error("HDFS ile permission updates failed ...");
                    result.setHdfsStatus(false);
                }

                //is file deletion possible
                if(hdfs.deleteFile())
                    logger.info("HDFS delete file successful !");
                else {
                    logger.error("HDFS delete file failed ...");
                    result.setHdfsStatus(false);
                }

                // clean up
                logger.info("tests complete.. clean up in progress .. !");
                hdfs.cleanup();
                logger.info("clean-up complete.. ");
            }
            else {
                logger.error("HDFS test folder cannot be created... ");
                result.setHdfsStatus(false);
            }
        }
    }

    /**
     * Set of Zookeeper tests
     * @param properties The cluster properties
     */
    private static void zookeeperProbe(Map<String, String> properties) {
        // zookeeper tests begin
        result.setZookeeperStatus(true);
        ProbeZookeeper zookeeper = new ProbeZookeeper(properties);
        if(zookeeper.isReachable())
            logger.info("Zookeeper is reachable...");
        else {
            logger.error("Zookeeper is not reachable ... ");
            result.setZookeeperStatus(false);
        }

        try {
            if(zookeeper.createZNodeData(properties.get("zkPath"), properties.get("zkData").getBytes())){
                logger.info("ZNode creation successful");

                if(zookeeper.getZNodeData(properties.get("zkPath")))
                    logger.info("ZNode data is readable");
                else {
                    logger.error("ZNode data read failed ...");
                    result.setZookeeperStatus(false);
                }

                if(zookeeper.updateZNodeData(properties.get("zkPath"), properties.get("zkData").getBytes())){
                    logger.info("ZNode data update successful");
                } else {
                    logger.error("ZNode data update failed ...");
                    result.setZookeeperStatus(false);
                }

                if(zookeeper.deleteZNodeData(properties.get("zkPath"))){
                    logger.info("ZNode data delete successful");
                } else {
                    logger.error("ZNode data delete failed ...");
                    result.setZookeeperStatus(false);
                }
            }
            else {
                logger.error("ZNode creation failed ...");
                result.setZookeeperStatus(false);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            result.setZookeeperStatus(false);
        }
    }


    /**
     * Set of HBase tests
     * @param properties The cluster properties
     */
    private static void probeHBase(Map<String, String> properties) {

        result.setHbaseStatus(true);
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, properties.get("zkQuorum"));
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, properties.get("zkPort"));
        conf.set(HConstants.HBASE_DIR, properties.get("hbaseDataDir"));
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, properties.get("hbaseZnodeParent"));
        ProbeHBase hbase = new ProbeHBase(conf);

        logger.info("beginning HBase tests... ");

        if (!hbase.isReachable(conf)) {
            logger.error("HBase is not reachable, exiting... ");
            result.setHbaseStatus(false);
        } else {
            logger.info("HBase service is reachable... ");
        }


        if (!hbase.createNameSpace(properties.get("hbaseNS"))){
            logger.error("HBase Namespace creation failed, exiting... ");
            result.setHbaseStatus(false);
        } else {
            logger.info("Namespace creation successful");
        }


        if (!hbase.createTable(properties.get("hbaseNS"), properties.get("hbaseTable"),
                properties.get("hbaseCF"))) {
            logger.error("Hbase table creation failed, exiting... ");
            result.setHbaseStatus(false);
        } else {
            logger.info("Table creation successful");
        }


        if (!hbase.writeToTable(properties.get("hbaseNS"),
                TableName.valueOf(properties.get("hbaseTable")),
                properties.get("hbaseCF").getBytes())){
            logger.error("HBase write to table failed, exiting... ");
            result.setHbaseStatus(false);
        } else {
            logger.info("Write to table successful");
        }

        if (!hbase.deleteNameSpace(properties.get("hbaseNS"))){
            logger.error("HBase namespace deletion failed, exiting... ");
            result.setHbaseStatus(false);
        } else {
            logger.info("Namespace deletion successful... ");
        }

        hbase.closeConnection();
        logger.info("HBase tests are successful.. ");
    }

    /**
     * set of hive tests
     * @param properties the cluster properties
     */
    private static void probeHive(Map<String, String> properties) {
        result.setHiveStatus(true);

        try {
            Connection hiveConnection = DriverManager.getConnection(properties.get("hiveJDBCURL"), "", "");
            Statement stm = hiveConnection.createStatement();
            ProbeHive hive = new ProbeHive(properties);

            if(!hive.createDatabase(properties.get("hiveDatabase"))) {
                logger.error("Hive database not created, exiting... ");
                result.setHiveStatus(false);
            } else {
                logger.info("Hive database was successfully created");
            }

            if(!hive.createTable(properties.get("hiveTableCreateStmt"),
                    properties.get("hiveDatabase"), properties.get("hiveTable"))) {
                logger.error("Hive table not created, exiting... ");
                result.setHiveStatus(false);
            } else {
                logger.info("Hive table was successfully created");
            }

            hive.writeToTable(properties);

            ResultSet rs = stm.executeQuery("select * from " +
                    String.join(".", properties.get("hiveDatabase"), properties.get("hiveTable")));
            Map<Integer, String> resultMap = new HashMap<>();
            while(rs.next()){
                resultMap.put(rs.getInt("key"), rs.getString("value"));
            }

            if(resultMap.size() == 2 && resultMap.get(1).equals("one")
                    && resultMap.get(2).equals("two"))
                logger.info("Loading data to hive was successful");
            else {
                logger.error("Loading data to hive failed, exiting... ");
                result.setHiveStatus(false);
            }

            if(!hive.dropTable(properties.get("hiveDatabase"), properties.get("hiveTable"))) {
                logger.error("Hive Drop table failed, exiting... ");
                result.setHiveStatus(false);
            } else {
                logger.info("Hive Drop table was successful");
            }

            if(!hive.dropDatabase(properties.get("hiveDatabase"), false)) {
                logger.error("Hive drop database failed, exiting... ");
                result.setHiveStatus(false);
            } else {
                logger.info("Hive drop database successful");
            }

            logger.info("Hive tests successfully completed");

        } catch (SQLException throwables) {
            throwables.printStackTrace();
            result.setHiveStatus(false);
        }
    }
}