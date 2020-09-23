package com.gautam.mantra.spark.extras;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.lang.invoke.MethodHandles;
import java.util.Map;

public class RunExtras {
    public final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    private final Map<String, String> properties;

    public RunExtras (Map<String, String> properties){
        this.properties = properties;
    }

    /**
     * This method submits a spark job to YARN
     * @return True if the job was successful, false otherwise
     */
    public boolean submitBixiBusiestStationJob(){
        System.setProperty("hdp.version", "3.1.0.0-78");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(properties.get("spark2Master"));
        sparkConf.setAppName(properties.get("bixi.busiest.station"));
        sparkConf.set("spark.submit.deployMode", properties.get("spark2DeployMode"));

        sparkConf.set("spark.driver.extraJavaOptions", properties.get("spark.driver.extraJavaOptions"));
        sparkConf.set("spark.yarn.am.extraJavaOptions", properties.get("spark.yarn.am.extraJavaOptions"));

        final String[] args = new String[]{
                "--jar",
                properties.get("sparkHDFSjar"),
                "--class",
                "com.gautam.mantra.spark.extras.BixiBusiestStation"
        };

        ClientArguments clientArguments = new ClientArguments(args);
        Client client = new Client(clientArguments, sparkConf);

        logger.info("submitting spark application to YARN :: " + properties.get("bixi.busiest.station"));

        ApplicationId applicationId = client.submitApplication();

        logger.info("application id is ::" + applicationId.toString());

        Tuple2<YarnApplicationState, FinalApplicationStatus> result =
                client.monitorApplication(applicationId, false,
                        Boolean.parseBoolean(properties.get("spark2YarnJobStatus")), 3000L);

        logger.info(String.format("final status of spark job :: %s :: %s",
                properties.get("bixi.busiest.station"), result._2.toString()));

        return result._2.toString().equals("SUCCEEDED");
    }

    /**
     * This method submits a spark job to YARN
     * @return True if the job was successful, false otherwise
     */
    public boolean submitTop10LongestTripsJob(){
        System.setProperty("hdp.version", "3.1.0.0-78");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(properties.get("spark2Master"));
        sparkConf.setAppName(properties.get("bixi.longest.trips"));
        sparkConf.set("spark.submit.deployMode", properties.get("spark2DeployMode"));

        sparkConf.set("spark.driver.extraJavaOptions", properties.get("spark.driver.extraJavaOptions"));
        sparkConf.set("spark.yarn.am.extraJavaOptions", properties.get("spark.yarn.am.extraJavaOptions"));

        final String[] args = new String[]{
                "--jar",
                properties.get("sparkHDFSjar"),
                "--class",
                "com.gautam.mantra.spark.extras.Top10LongestRides"
        };

        ClientArguments clientArguments = new ClientArguments(args);
        Client client = new Client(clientArguments, sparkConf);

        logger.info("submitting spark application to YARN :: " + properties.get("bixi.longest.trips"));

        ApplicationId applicationId = client.submitApplication();

        logger.info("application id is ::" + applicationId.toString());

        Tuple2<YarnApplicationState, FinalApplicationStatus> result =
                client.monitorApplication(applicationId, false,
                        Boolean.parseBoolean(properties.get("spark2YarnJobStatus")), 3000L);

        logger.info(String.format("final status of spark job :: %s :: %s",
                properties.get("bixi.longest.trips"), result._2.toString()));

        return result._2.toString().equals("SUCCEEDED");
    }

    /**
     * This method submits a spark job to YARN
     * @return True if the job was successful, false otherwise
     */
    public boolean submitMonthlyAverageJob(){
        System.setProperty("hdp.version", "3.1.0.0-78");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(properties.get("spark2Master"));
        sparkConf.setAppName(properties.get("bixi.monthly.averages"));
        sparkConf.set("spark.submit.deployMode", properties.get("spark2DeployMode"));

        sparkConf.set("spark.driver.extraJavaOptions", properties.get("spark.driver.extraJavaOptions"));
        sparkConf.set("spark.yarn.am.extraJavaOptions", properties.get("spark.yarn.am.extraJavaOptions"));

        final String[] args = new String[]{
                "--jar",
                properties.get("sparkHDFSjar"),
                "--class",
                "com.gautam.mantra.spark.extras.AverageRideDurationPerMonth"
        };

        ClientArguments clientArguments = new ClientArguments(args);
        Client client = new Client(clientArguments, sparkConf);

        logger.info("submitting spark application to YARN :: " + properties.get("bixi.monthly.averages"));

        ApplicationId applicationId = client.submitApplication();

        logger.info("application id is ::" + applicationId.toString());

        Tuple2<YarnApplicationState, FinalApplicationStatus> result =
                client.monitorApplication(applicationId, false,
                        Boolean.parseBoolean(properties.get("spark2YarnJobStatus")), 3000L);

        logger.info(String.format("final status of spark job :: %s :: %s",
                properties.get("bixi.monthly.averages"), result._2.toString()));

        return result._2.toString().equals("SUCCEEDED");
    }
}
