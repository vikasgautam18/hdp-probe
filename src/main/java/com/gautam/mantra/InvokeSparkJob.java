package com.gautam.mantra;


import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;

public class InvokeSparkJob {
    public static void main(String[] args) throws IOException, YarnException {
        //withSparkLauncher();

        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        YarnClient client = YarnClient.createYarnClient();
        client.init(yarnConfiguration);

        client.start();

        System.out.println(client.getApplications().size());

        ApplicationReport report = client.getApplicationReport(ApplicationId.fromString("application_1603215043575_0003"));

        System.out.println(report.getYarnApplicationState().name());
        if (!report.getApplicationTags().isEmpty()) {
            report.getApplicationTags().forEach(System.out::println);
        }

        client.getApplications(new HashSet<>(Collections.singleton("SPARK")),
                EnumSet.of(YarnApplicationState.FINISHED),
                new HashSet<>(Collections.singleton("SparkPi")))
        .forEach(applicationReport -> System.out.println(applicationReport.getName()));
    }

    public static void withSparkLauncher() throws IOException, InterruptedException {
        SparkLauncher sparkLauncher = new SparkLauncher();

        SparkAppHandle handle = sparkLauncher
                .setSparkHome("/usr/hdp/3.1.0.0-78/spark2")
                .setAppResource("/usr/hdp/3.1.0.0-78/spark2/examples/jars/spark-examples_2.11-2.3.2.3.1.0.0-78.jar")
                .setMainClass("org.apache.spark.examples.SparkPi")
                //.setMaster("local[*]")
                .setMaster("yarn").setDeployMode("cluster")
                .startApplication();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        handle.addListener(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle handle) {
                if (handle.getState().isFinal()) {
                    countDownLatch.countDown();
                }
            }
            @Override
            public void infoChanged(SparkAppHandle handle) {
            }
        });
        countDownLatch.await();

        System.out.println(handle.getAppId() + " ended in state " + handle.getState());
    }
}
