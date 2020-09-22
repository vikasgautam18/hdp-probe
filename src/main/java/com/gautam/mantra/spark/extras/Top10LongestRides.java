package com.gautam.mantra.spark.extras;

/**
 * Download the dataset below:
 * https://sitewebbixi.s3.amazonaws.com/uploads/docs/biximontrealrentals2019-33ea73.zip
 *
 * Goal is to find top 10 longest rides for 2019
 */
public class Top10LongestRides {


    public static void main(String[] args) {
        if(args.length != 0){
            System.out.println("USAGE: spark-submit --driver-java-options \"-Dspark.probe.cluster.yml=conf/cluster-conf.yml\" " +
                    "--class com.gautam.mantra.spark.extras.Top10LongestRides target/hdp-probe.jar");
        }


    }

}
