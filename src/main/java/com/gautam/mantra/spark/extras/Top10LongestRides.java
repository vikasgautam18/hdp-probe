package com.gautam.mantra.spark.extras;

import com.gautam.mantra.commons.Utilities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.desc;

/**
 * Download the dataset below:
 * https://sitewebbixi.s3.amazonaws.com/uploads/docs/biximontrealrentals2019-33ea73.zip
 *
 * Goal is to find top 10 longest rides for 2019
 */
public class Top10LongestRides implements Serializable {

    public static final Yaml yaml = new Yaml();
    private static final String CLUSTER_CONFIG = "spark.probe.cluster.yml";
    private static final String APP_NAME = "bixi.busiest.station";
    private static final String BIXI_DATASET_PATH = "bixi.dataset.path";
    private static final String BIXI_STATION = "bixi.station.file.name";
    public static Map<String, String> stationMap;

    public static void main(String[] args) {
        if(args.length != 0){
            System.out.println("USAGE: spark-submit --driver-java-options \"-Dspark.probe.cluster.yml=conf/cluster-conf.yml\" " +
                    "--class com.gautam.mantra.spark.extras.Top10LongestRides target/hdp-probe.jar");
        }

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

            populateStationMap(stations);

            System.out.println("Top 10 longest trips are:: ");
            getTop10LongestTrips(trips, spark).forEach(Top10LongestRides::printTrip);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void printTrip(Row row) {
        System.out.printf(" Trip starting from '%s' to '%s' of duration %s seconds.%n",
                row.getAs("start_station_name"),
                row.getAs("end_station_name"),
                row.getAs("duration_sec"));
    }

    public static void populateStationMap(Dataset<Row> stations) {
        stationMap = new HashMap<>();
        stations
                .select("Code", "name")
                .collectAsList()
                .forEach(row -> stationMap.put(row.getAs("Code"), row.getAs("name")));
    }

    public static List<Row> getTop10LongestTrips(Dataset<Row> trips, SparkSession spark) {
        spark.udf().register("lookUpStationName", lookUpStationName(), DataTypes.StringType);
        return trips.withColumn("start_station_name",
                functions.callUDF("lookUpStationName", trips.col("start_station_code")))
                .withColumn("end_station_name",
                        functions.callUDF("lookUpStationName", trips.col("end_station_code")))
                .select("start_station_name", "end_station_name", "duration_sec")
                .orderBy(desc("duration_sec")).takeAsList(10);
    }

    public static UDF1<String, String> lookUpStationName(){
        return (stationCode) -> stationMap.get(stationCode);
    }

    public static Dataset<Row> getTripsDataset(SparkSession spark, String path) {
        Dataset<Row> trips = spark.read()
                .format("csv").option("header", "true").load(path);

        System.out.println("Trips data sneak peek");
        trips.show(10);

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
