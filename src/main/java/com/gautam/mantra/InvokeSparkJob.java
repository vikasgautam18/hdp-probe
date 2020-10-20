package com.gautam.mantra;


import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class InvokeSparkJob {
    public static void main(String[] args) throws IOException, YarnException {
        //withSparkLauncher();

        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        YarnClient client = YarnClient.createYarnClient();
        client.init(yarnConfiguration);

        client.start();

        System.out.println(client.getApplications().size());
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
