package com.gautam.mantra.spark;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.apache.spark.sql.SparkSession;
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

        sparkConf.set("spark.driver.extraJavaOptions", "-Dhdp.version=3.1.0.0-78 -Dspark2hdfs.cluster.yml="+ properties.get("clusterPropsFile"));
        sparkConf.set("spark.yarn.am.extraJavaOptions", "-Dhdp.version=3.1.0.0-78");

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
        sparkConf.set("spark.driver.extraLibraryPath",
                "/usr/hdp/current/hadoop-client/lib/native:/usr/hdp/current/hadoop-client/lib/native/Linux-amd64-64");
        sparkConf.set("spark.executor.extraLibraryPath",
                "/usr/hdp/current/hadoop-client/lib/native:/usr/hdp/current/hadoop-client/lib/native/Linux-amd64-64");
        sparkConf.set("spark.driver.extraJavaOptions",
                "-Dhdp.version=3.1.0.0-78 -Dspark2hive.cluster.yml="+ properties.get("clusterPropsFile"));
        sparkConf.set("spark.yarn.am.extraJavaOptions", "-Dhdp.version=3.1.0.0-78");
        sparkConf.set("spark.driver.extraClassPath", "/usr/hdp/3.1.0.0-78/spark2/jars/*");
        sparkConf.set("spark.sql.hive.metastore.jars", "/usr/hdp/current/spark2-client/standalone-metastore/*");
        sparkConf.set("spark.sql.hive.metastore.version", "3.0");
        sparkConf.set("spark.sql.warehouse.dir", "/apps/spark/warehouse");

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

    private boolean verifySparkSQLJobResult(Map<String, String> properties) {

        SparkSession spark = SparkSession.builder()
                .appName(properties.get("sparkHiveAppName"))
                .enableHiveSupport()
                .config("job.local.dir", "/tmp/")
                .config("spark.sql.warehouse.dir", "/tmp/")
                .config("spark.driver.extraLibraryPath",
                    "/usr/hdp/current/hadoop-client/lib/native:/usr/hdp/current/hadoop-client/lib/native/Linux-amd64-64:" +
                            "/usr/hdp/3.1.0.0-78/spark2/jars/spark-hive_2.11-2.3.2.3.1.0.0-78.jar")
                .config("spark.executor.extraLibraryPath",
                "/usr/hdp/current/hadoop-client/lib/native:/usr/hdp/current/hadoop-client/lib/native/Linux-amd64-64:" +
                        "/usr/hdp/3.1.0.0-78/spark2/jars/spark-hive_2.11-2.3.2.3.1.0.0-78.jar")
                .config("spark.driver.extraJavaOptions", "-Dhdp.version=3.1.0.0-78 -Dfs.hdfs.impl=org.apache.hadoop.hdfs.DistributedFileSystem " +
                        "-Dfs.file.impl=org.apache.hadoop.fs.LocalFileSystem")
                .config("spark.yarn.am.extraJavaOptions", "-Dhdp.version=3.1.0.0-78 -Dfs.hdfs.impl=org.apache.hadoop.hdfs.DistributedFileSystem " +
                        "-Dfs.file.impl=org.apache.hadoop.fs.LocalFileSystem")
                .config("spark.driver.extraClassPath", "/usr/hdp/3.1.0.0-78/spark2/jars/*:/usr/hdp/3.1.0.0-78/spark2/jars/")
                .config("spark.sql.hive.metastore.jars", "/usr/hdp/current/spark2-client/standalone-metastore/*")
                .config("spark.sql.hive.metastore.version", "3.0")
                .config("spark.sql.warehouse.dir", "/apps/spark/warehouse")
                .config("spark.submit.deployMode", "client")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        spark.sparkContext().hadoopConfiguration().set("fs.file.impl","org.apache.hadoop.fs.LocalFileSystem");

        String finalTableName = properties.get("sparkHiveDB") + "." + properties.get("sparkHiveTable");
        boolean result = spark.sql("select * from " + finalTableName).count() == Integer.parseInt(properties.get("sparkHiveNumRecords"));
        spark.stop();
        return result;
    }
}