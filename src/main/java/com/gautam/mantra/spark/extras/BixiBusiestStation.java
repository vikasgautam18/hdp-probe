package com.gautam.mantra.spark.extras;

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


/**
 * Download the dataset below:
 * https://sitewebbixi.s3.amazonaws.com/uploads/docs/biximontrealrentals2019-33ea73.zip
 *
 * Goal is to find the busiest station which is the station with most number of trips starting from
 */
public class BixiBusiestStation {

    public static final Yaml yaml = new Yaml();
    private static final String CLUSTER_CONFIG = "spark.probe.cluster.yml";
    private static final String APP_NAME = "bixi.busiest.station";
    private static final String BIXI_DATASET_PATH = "bixi.dataset.path";
    private static final String BIXI_STATION = "bixi.station.file.name";

    public static void main(String[] args) {
        if(args.length != 0){
            System.out.println("USAGE: spark-submit --driver-java-options \"-Dspark.probe.cluster.yml=conf/cluster-conf.yml\" " +
                    "--class com.gautam.mantra.spark.extras.BixiBusiestStation target/hdp-probe.jar");
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
            Dataset<Row> stations = getStationDataset(spark,
                    properties.get(BIXI_DATASET_PATH) + "/" + properties.get(BIXI_STATION));

            // dataframe of Trips
            Dataset<Row> trips = getTripsDataset(spark, properties.get(BIXI_DATASET_PATH ) + "/trips/*");

            System.out.printf("The busiest station in 2019 was ::%s%n", findBusiestStation(stations, trips));

            spark.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static String findBusiestStation(Dataset<Row> stations, Dataset<Row> trips) {
        String station_code = trips.groupBy("start_station_code")
                .count().sort(desc("count")).first().getAs("start_station_code");

        // find station code with highest number of trips
        return stations.filter("Code=" + station_code).first().getAs("name");
    }

    public static Dataset<Row> getTripsDataset(SparkSession spark, String path) {
        Dataset<Row> trips = spark.read()
                .format("csv").option("header", "true").load(path);

        System.out.println("Trips data sneak peek");
        trips.show(10);

        // group trips by station code
        System.out.println("Schema of trip dataset");
        trips.printSchema();
        return trips;
    }

    public static Dataset<Row> getStationDataset(SparkSession spark, String path) {
        Dataset<Row> stations = spark.read().format("csv").option("header", "true")
                .load(path);

        System.out.println("schema of Stations dataset::");
        stations.printSchema();

        System.out.println("Stations dataset sneak peek:: ");
        stations.show(10);
        return stations;
    }
}
