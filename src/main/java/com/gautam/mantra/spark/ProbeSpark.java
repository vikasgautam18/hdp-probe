package com.gautam.mantra.spark;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class ProbeSpark {
    public final Logger logger = LoggerFactory.getLogger(ProbeSpark.class.getName());

    public boolean submitPiExampleJob(Map<String, String> properties){
        System.setProperty("SPARK_YARN_MODE", "true");
        System.setProperty("hdp.version", "3.1.0.0-78");
        System.setProperty("SPARK_HOME", properties.get("spark2Home"));

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

    public Dataset<Row> generateRandomDF(int numRows){

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("event_id", DataTypes.StringType, false),
                DataTypes.createStructField("event_ts", TimestampType, false)
        });

        SparkSession spark = SparkSession
                .builder()
                .appName("Build a DataFrame from Scratch")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().schema(schema).csv(spark.emptyDataset(Encoders.STRING()));

        dataset.printSchema();
        dataset.show();
        return null;
    }
}