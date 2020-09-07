package com.gautam.mantra.spark;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Map;

public class ProbeSpark {
    public final Logger logger = LoggerFactory.getLogger(ProbeSpark.class.getName());

    public boolean submitJob(Map<String, String> properties){
        System.setProperty("SPARK_YARN_MODE", "true");
        System.setProperty("hdp.version", "3.1.0.0-78");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setSparkHome(properties.get("spark2Home"));

        sparkConf.setMaster("yarn");
        sparkConf.setAppName("spark-yarn");
        sparkConf.set("master", "yarn");
        sparkConf.set("spark.submit.deployMode", "cluster");

        sparkConf.set("spark.driver.extraJavaOptions", "-Dhdp.version=3.1.0.0-78");
        sparkConf.set("spark.yarn.am.extraJavaOptions", "-Dhdp.version=3.1.0.0-78");

        final String[] args = new String[]{
                "--jar",
                properties.get("sparkJarFile"),
                "--class",
                "org.apache.spark.examples.SparkPi"
        };

        ClientArguments clientArguments = new ClientArguments(args);
        Client client = new Client(clientArguments, sparkConf);
        logger.info("submitting application:: ");
        ApplicationId applicationId = client.submitApplication();
        logger.info("application id is ::" + applicationId.toString());

        Tuple2<YarnApplicationState, FinalApplicationStatus> result =
                client.monitorApplication(applicationId, false, true, 3000L);

        logger.info("final status:: " + result._2.toString());

        return true;
    }
}