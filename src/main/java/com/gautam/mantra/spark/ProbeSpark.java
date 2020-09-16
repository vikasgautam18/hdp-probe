package com.gautam.mantra.spark;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ProbeSpark {
    public final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

    /**
     * This method submits a spark job to YARN
     * @param properties the cluster configuration
     * @return True if the job was successful, false otherwise
     */
    public boolean submitPiExampleJob(Map<String, String> properties){
        System.setProperty("SPARK_YARN_MODE", "true");
        System.setProperty("hdp.version", "3.1.0.0-78");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setSparkHome(properties.get("spark2Home"));
        sparkConf.setMaster(properties.get("spark2Master"));
        sparkConf.setAppName(properties.get("spark2AppName"));
        sparkConf.set("spark.submit.deployMode", properties.get("spark2DeployMode"));

        sparkConf.set("spark.driver.extraJavaOptions", "-Dhdp.version=3.1.0.0-78");
        sparkConf.set("spark.yarn.am.extraJavaOptions", "-Dhdp.version=3.1.0.0-78");

        final String[] args = new String[]{
                "--jar",
                properties.get("spark2JarFile"),
                "--class",
                "org.apache.spark.examples.SparkPi"
        };

        ClientArguments clientArguments = new ClientArguments(args);
        Client client = new Client(clientArguments, sparkConf);

        logger.info("submitting spark pi example application to YARN :: ");

        ApplicationId applicationId = client.submitApplication();

        logger.info("application id is ::" + applicationId.toString());

        Tuple2<YarnApplicationState, FinalApplicationStatus> result =
                client.monitorApplication(applicationId, false,
                        Boolean.parseBoolean(properties.get("spark2YarnJobStatus")), 3000L);

        logger.info("final status of spark pi example job :: " + result._2.toString());

        return result._2.toString().equals("SUCCEEDED");
    }

    /**
     * This method submits a spark job to YARN
     * @param properties the cluster configuration
     * @return True if the job was successful, false otherwise
     */
    public boolean submitHDFSJob(Map<String, String> properties){
        System.setProperty("hdp.version", "3.1.0.0-78");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(properties.get("spark2Master"));
        sparkConf.setAppName(properties.get("sparkHDFSAppName"));
        sparkConf.set("spark.submit.deployMode", properties.get("spark2DeployMode"));

        sparkConf.set("spark.driver.extraJavaOptions", properties.get("spark.driver.extraJavaOptions"));
        sparkConf.set("spark.yarn.am.extraJavaOptions", properties.get("spark.yarn.am.extraJavaOptions"));

        final String[] args = new String[]{
                "--jar",
                properties.get("sparkHDFSjar"),
                "--class",
                "com.gautam.mantra.spark.SparkHDFSProbe"
        };

        ClientArguments clientArguments = new ClientArguments(args);
        Client client = new Client(clientArguments, sparkConf);

        logger.info("submitting spark hdfs probe application to YARN :: ");

        ApplicationId applicationId = client.submitApplication();

        logger.info("application id is ::" + applicationId.toString());

        Tuple2<YarnApplicationState, FinalApplicationStatus> result =
                client.monitorApplication(applicationId, false,
                        Boolean.parseBoolean(properties.get("spark2YarnJobStatus")), 3000L);

        logger.info("final status of spark hdfs probe job :: " + result._2.toString());

        return result._2.toString().equals("SUCCEEDED") && verifyHDFSJobResult(properties);
    }

    /**
     * This method verifies the result in HDFS by counting the number of rows
     * @param properties the cluster configuration
     * @return true if the HDFS file is accurately created, false otherwise
     */
    private boolean verifyHDFSJobResult(Map<String, String> properties) {
        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", properties.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try{
            FileSystem fs = FileSystem.get(URI.create(properties.get("hdfsPath")), conf);
            if(fs.exists(new Path(properties.get("sparkHDFSFinalFile")))){
                FSDataInputStream inputStream = fs.open(new Path(properties.get("sparkHDFSFinalFile")));
                String content = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                return countLines(content) == Integer.parseInt(properties.get("sparkHDFSNumRecords")) + 1 ;
            }
            else {
                logger.error("File does not exist !");
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * A small utility for counting the total rows
     * @param str the input string
     * @return
     */
    private int countLines(String str){
        String[] lines = str.split("\r\n|\r|\n");
        return  lines.length;
    }


    /**
     * This method submits a spark job to YARN
     * @param properties the cluster configuration
     * @return True if the job was successful, false otherwise
     */
    public boolean submitSparkSQLJob(Map<String, String> properties){
        System.setProperty("hdp.version", "3.1.0.0-78");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(properties.get("spark2Master"));
        sparkConf.setAppName(properties.get("sparkHiveAppName"));
        sparkConf.set("spark.submit.deployMode", properties.get("spark2DeployMode"));
        sparkConf.set("spark.driver.extraLibraryPath", properties.get("spark.driver.extraLibraryPath"));
        sparkConf.set("spark.executor.extraLibraryPath", properties.get("spark.executor.extraLibraryPath"));
        sparkConf.set("spark.driver.extraJavaOptions", properties.get("spark.driver.extraJavaOptions"));
        sparkConf.set("spark.yarn.am.extraJavaOptions", properties.get("spark.yarn.am.extraJavaOptions"));
        sparkConf.set("spark.driver.extraClassPath", properties.get("spark.driver.extraClassPath"));
        sparkConf.set("spark.sql.hive.metastore.jars", properties.get("spark.sql.hive.metastore.jars"));
        sparkConf.set("spark.sql.hive.metastore.version", properties.get("spark.sql.hive.metastore.version"));
        sparkConf.set("spark.sql.warehouse.dir", properties.get("spark.sql.warehouse.dir"));


        final String[] args = new String[]{
                "--jar",
                properties.get("sparkHivejar"),
                "--class",
                "com.gautam.mantra.spark.SparkSQLProbe"
        };

        ClientArguments clientArguments = new ClientArguments(args);
        Client client = new Client(clientArguments, sparkConf);

        logger.info("submitting spark sql probe application to YARN :: ");

        ApplicationId applicationId = client.submitApplication();

        logger.info("application id is ::" + applicationId.toString());

        Tuple2<YarnApplicationState, FinalApplicationStatus> result =
                client.monitorApplication(applicationId, false,
                        Boolean.parseBoolean(properties.get("spark2YarnJobStatus")), 3000L);

        logger.info("final status of spark sql probe job :: " + result._2.toString());

        return result._2.toString().equals("SUCCEEDED") && verifySparkSQLJobResult(properties);
    }

    /**
     * this method verifies the result of sparksql spark job
     * @param properties the cluster configuration
     * @return returns true if the job was successful, false otherwise
     */
    private boolean verifySparkSQLJobResult(Map<String, String> properties) {

        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", properties.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try{
            FileSystem fs = FileSystem.get(URI.create(properties.get("hdfsPath")), conf);
            if(fs.exists(new Path(properties.get("sparkSQLExportFile")))){
                FSDataInputStream inputStream = fs.open(new Path(properties.get("sparkSQLExportFile")));
                String content = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                return countLines(content) == Integer.parseInt(properties.get("sparkHiveNumRecords"));
            }
            else {
                logger.error("File does not exist !");
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }



    /**
     * This method submits a spark job to YARN
     * @param properties the cluster configuration
     * @return True if the job was successful, false otherwise
     */
    public boolean submitSparkHiveJob(Map<String, String> properties){
        System.setProperty("hdp.version", "3.1.0.0-78");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(properties.get("spark2Master"));
        sparkConf.setAppName(properties.get("sparkHiveAppName"));
        sparkConf.set("spark.submit.deployMode", properties.get("spark2DeployMode"));
        sparkConf.set("spark.driver.extraLibraryPath", properties.get("spark.driver.extraLibraryPath"));
        sparkConf.set("spark.executor.extraLibraryPath", properties.get("spark.executor.extraLibraryPath"));
        sparkConf.set("spark.driver.extraJavaOptions", properties.get("spark.driver.extraJavaOptions"));
        sparkConf.set("spark.yarn.am.extraJavaOptions", properties.get("spark.yarn.am.extraJavaOptions"));
        sparkConf.set("spark.driver.extraClassPath", properties.get("spark.driver.extraClassPath"));
        sparkConf.set("spark.sql.hive.metastore.jars", properties.get("spark.sql.hive.metastore.jars"));
        sparkConf.set("spark.sql.hive.metastore.version", properties.get("spark.sql.hive.metastore.version"));
        sparkConf.set("spark.sql.warehouse.dir", properties.get("spark.hive.warehouse.dir"));

        final String[] args = new String[]{
                "--jar",
                properties.get("sparkHivejar"),
                "--class",
                "com.gautam.mantra.spark.SparkHiveProbe"
        };

        ClientArguments clientArguments = new ClientArguments(args);
        Client client = new Client(clientArguments, sparkConf);

        logger.info("submitting spark hive application to YARN :: ");

        ApplicationId applicationId = client.submitApplication();

        logger.info("application id is ::" + applicationId.toString());

        Tuple2<YarnApplicationState, FinalApplicationStatus> result =
                client.monitorApplication(applicationId, false,
                        Boolean.parseBoolean(properties.get("spark2YarnJobStatus")), 3000L);

        logger.info("final status of spark hive probe job :: " + result._2.toString());

        return result._2.toString().equals("SUCCEEDED") && verifySparkHiveJobResult(properties);
    }

    /**
     * this method verifies the result of spark-hive spark job
     * @param properties the cluster configuration
     * @return returns true if the job was successful, false otherwise
     */
    private boolean verifySparkHiveJobResult(Map<String, String> properties) {

        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", properties.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try{
            FileSystem fs = FileSystem.get(URI.create(properties.get("hdfsPath")), conf);
            if(fs.exists(new Path(properties.get("sparkHiveExportFile")))){
                FSDataInputStream inputStream = fs.open(new Path(properties.get("sparkHiveExportFile")));
                String content = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                return countLines(content) == Integer.parseInt(properties.get("sparkHiveNumRecords"));
            }
            else {
                logger.error("File does not exist !");
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method submits a spark job to YARN
     * @param properties the cluster configuration
     * @return True if the job was successful, false otherwise
     */
    public boolean submitSparkHBaseJob(Map<String, String> properties){
        TableName tableName = TableName.valueOf(properties.get("sparkHBaseTableName"));
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, properties.get("zkQuorum"));
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, properties.get("zkPort"));
        conf.set(HConstants.HBASE_DIR, properties.get("hbaseDataDir"));
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, properties.get("hbaseZnodeParent"));
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            if(!connection.getAdmin().tableExists(tableName)){
                //table does not exist. create it
                connection.getAdmin().createTable(TableDescriptorBuilder
                        .newBuilder(tableName)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("Office"))
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("Personal"))
                        .build());
            } /*else
            {
                //table exists - empty it

                connection.getAdmin().disableTable(tableName);
                connection.getAdmin().truncateTable(tableName, false);
                connection.getAdmin().enableTable(tableName);
            }*/
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.setProperty("hdp.version", "3.1.0.0-78");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(properties.get("spark2Master"));
        sparkConf.setAppName(properties.get("sparkHiveAppName"));
        sparkConf.set("spark.submit.deployMode", properties.get("spark2DeployMode"));
        //sparkConf.set("spark.driver.extraLibraryPath", properties.get("spark.driver.extraLibraryPath"));
        //sparkConf.set("spark.executor.extraLibraryPath", properties.get("spark.executor.extraLibraryPath"));
        sparkConf.set("spark.driver.extraJavaOptions", properties.get("spark.driver.extraJavaOptions"));
        sparkConf.set("spark.yarn.am.extraJavaOptions", properties.get("spark.yarn.am.extraJavaOptions"));
        sparkConf.set("spark.driver.extraClassPath", properties.get("spark.driver.extraClassPath"));
        sparkConf.set("spark.executor.extraClassPath", properties.get("spark.executor.extraClassPath"));
        //sparkConf.set("spark.sql.hive.metastore.jars", properties.get("spark.sql.hive.metastore.jars"));
        //sparkConf.set("spark.sql.hive.metastore.version", properties.get("spark.sql.hive.metastore.version"));
        //sparkConf.set("spark.sql.warehouse.dir", properties.get("spark.hive.warehouse.dir"));

        final String[] args = new String[]{
                "--jar",
                properties.get("sparkHivejar"),
                "--class",
                "com.gautam.mantra.spark.SparkHBaseProbe"
        };

        ClientArguments clientArguments = new ClientArguments(args);
        Client client = new Client(clientArguments, sparkConf);

        logger.info("submitting spark hbase application to YARN :: ");

        ApplicationId applicationId = client.submitApplication();

        logger.info("application id is ::" + applicationId.toString());

        Tuple2<YarnApplicationState, FinalApplicationStatus> result =
                client.monitorApplication(applicationId, false,
                        Boolean.parseBoolean(properties.get("spark2YarnJobStatus")), 3000L);

        logger.info("final status of spark hbase probe job :: " + result._2.toString());

        return result._2.toString().equals("SUCCEEDED");
    }

    /**
     * this method verifies the result of spark-hive spark job
     * @param properties the cluster configuration
     * @return returns true if the job was successful, false otherwise
     */
    private boolean verifySparkHBaseJobResult(Map<String, String> properties) {

        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", properties.get("hdfsPath"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try{
            FileSystem fs = FileSystem.get(URI.create(properties.get("hdfsPath")), conf);
            if(fs.exists(new Path(properties.get("sparkHiveExportFile")))){
                FSDataInputStream inputStream = fs.open(new Path(properties.get("sparkHiveExportFile")));
                String content = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                return countLines(content) == Integer.parseInt(properties.get("sparkHiveNumRecords"));
            }
            else {
                logger.error("File does not exist !");
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}