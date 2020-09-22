package com.gautam.mantra.extras;

import com.gautam.mantra.commons.Utilities;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

import static org.apache.spark.sql.functions.desc;

public class BixiBusiestStation {
    /**
     * Download the dataset below:
     * https://sitewebbixi.s3.amazonaws.com/uploads/docs/biximontrealrentals2019-33ea73.zip
     *
     * Goal is to find the busiest station
     */

    public static final Yaml yaml = new Yaml();
    private static final String CLUSTER_CONFIG = "spark.extras.cluster.yml";
    private static final String APP_NAME = "bixi.busiest.station";
    private static final String BIXI_DATASET_PATH = "bixi.dataset.path";
    private static final String BIXI_STATION = "bixi.station.file.name";

    public static void main(String[] args) {

        if(args.length != 0){
            System.out.println("USAGE: spark-submit --driver-java-options \"-Dspark.extras.cluster.yml=conf/cluster-conf.yml\" " +
                    "--class com.gautam.mantra.extras.BixiBusiestStation target/hdp-probe.jar");
        }
        Logger.getRootLogger().setLevel(Level.ERROR);

        // load properties
        InputStream inputStream;
        try {
            inputStream = new FileInputStream(
                    new File(System.getProperty(CLUSTER_CONFIG)));
            Utilities utilities = new Utilities();

            Map<String, String> properties = yaml.load(inputStream);
            // print all loaded properties to console
            utilities.printProperties(properties);

            SparkSession spark = SparkSession.builder()
                    .appName(properties.get(APP_NAME)).getOrCreate();

            // dataframe of Stations
            Dataset<Row> stations = spark.read().format("csv").option("header", "true")
                    .load(properties.get(BIXI_DATASET_PATH) + "/" + properties.get(BIXI_STATION ));
            stations.show(10);

            // dataframe of Trips
            Dataset<Row> trips = spark.read()
                    .format("csv").option("header", "true").load(properties.get(BIXI_DATASET_PATH ) + "/trips/*");

            trips.show(10);

            // group trips by station code
            trips.printSchema();

            String station_code = trips.groupBy("start_station_code")
                    .count().sort(desc("count")).first().getAs("start_station_code");

            // find station code with highest number of trips
            String station_name = stations.filter("Code=" + station_code).first().getAs("name");

            // print result

            System.out.printf("The busiest station in 2019 was ::%s%n", station_name);

            spark.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
