package com.gautam.mantra.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Map;

public class ProbeSpark {
    Configuration config;
    public final Logger logger = LoggerFactory.getLogger(ProbeSpark.class.getName());

    public ProbeSpark(Map<String, String> properties){
        config = new Configuration();
        try {
            config.addResource(new File(properties.get("coreSiteLocation")).getAbsoluteFile().toURI().toURL());
            config.addResource(new File(properties.get("hdfsSiteLocation")).getAbsoluteFile().toURI().toURL());
            config.addResource(new File(properties.get("mapredSiteLocation")).getAbsoluteFile().toURI().toURL());
            config.addResource(new File(properties.get("yarnSiteLocation")).getAbsoluteFile().toURI().toURL());
            config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            config.set("hadoop.home.dir", properties.get("hadoopHomeDir"));
            config.set("hadoop.conf.dir", properties.get("hadoopConfDir"));
            config.set("yarn.conf.dir", properties.get("yarnConfDir"));

        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }


    public boolean submitJob(Map<String, String> properties){
        System.setProperty("SPARK_YARN_MODE", "true");
        SparkConf sparkConf = new SparkConf();
        sparkConf.setSparkHome(properties.get("spark2Home"));

        sparkConf.setMaster("yarn");
        sparkConf.setAppName("spark-yarn");
        sparkConf.set("master", "yarn");
        sparkConf.set("spark.submit.deployMode", "cluster");
        sparkConf.set("hdp.version", "3.1.0.0-78");
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
                client.monitorApplication(applicationId, true, true, 100L);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("final status:: " + result._2.toString());

        return true;
    }
}
