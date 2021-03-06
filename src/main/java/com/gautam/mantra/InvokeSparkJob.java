package com.gautam.mantra;


import com.gautam.mantra.commons.Utilities;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class InvokeSparkJob {
    public static final Yaml yaml = new Yaml();

    public static void main(String[] args) throws IOException, YarnException, InterruptedException {

        InputStream inputStream = new FileInputStream(
                new File(System.getProperty("spark.probe.cluster.yml")));
        Utilities utilities = new Utilities();

        Map<String, String> properties = yaml.load(inputStream);
        // print all loaded properties to console
        utilities.printProperties(properties);

        System.out.println("**********************************************************************************");

        withSparkLauncher();

        YarnConfiguration yarnConfiguration = new YarnConfiguration();

        YarnClient client = YarnClient.createYarnClient();
        client.init(yarnConfiguration);

        client.start();

        System.out.println(client.getApplications().size());

        ApplicationReport report =
                client.getApplicationReport(ApplicationId.fromString("application_1603215043575_0003"));

        System.out.println(report.getYarnApplicationState().name());
        if (!report.getApplicationTags().isEmpty()) {
            report.getApplicationTags().forEach(System.out::println);
        }

        client.getApplications(new HashSet<>(Collections.singleton("SPARK")),
                EnumSet.of(YarnApplicationState.FINISHED),
                new HashSet<>(Collections.singleton("sparkpi")))
        .forEach(applicationReport -> System.out.println(applicationReport.getName()));
    }

    public static void withSparkLauncher() throws IOException, InterruptedException {
        SparkLauncher sparkLauncher = new SparkLauncher();

        SparkAppHandle handle = sparkLauncher
                .setSparkHome("/usr/hdp/3.1.0.0-78/spark2")
                .setAppResource("/usr/hdp/3.1.0.0-78/spark2/examples/jars/spark-examples_2.11-2.3.2.3.1.0.0-78.jar")
                .setMainClass("org.apache.spark.examples.SparkPi")
                .setMaster("yarn").setDeployMode("cluster")
                .setConf("spark.yarn.tags", "sparkpi")
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
