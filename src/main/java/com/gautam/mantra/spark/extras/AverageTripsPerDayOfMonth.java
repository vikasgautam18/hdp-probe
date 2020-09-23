package com.gautam.mantra.spark.extras;

import com.gautam.mantra.commons.Utilities;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.Map;

import static org.apache.spark.sql.functions.asc;

/**
 * Download the dataset below:
 * https://sitewebbixi.s3.amazonaws.com/uploads/docs/biximontrealrentals2019-33ea73.zip
 *
 * Goal is to find average trip length per day of a month for 2019
 */
public class AverageTripsPerDayOfMonth implements Serializable {

    public static final Yaml yaml = new Yaml();
    private static final String CLUSTER_CONFIG = "spark.probe.cluster.yml";
    private static final String APP_NAME = "bixi.day0fmonth.averages";
    private static final String BIXI_DATASET_PATH = "bixi.dataset.path";

    public static void main(String[] args) {
        Logger.getRootLogger().setLevel(Level.ERROR);

        if(args.length != 0){
            System.out.println("USAGE: spark-submit --driver-java-options \"-Dspark.probe.cluster.yml=conf/cluster-conf.yml\" " +
                    "--class com.gautam.mantra.spark.extras.AverageTripsPerDayOfMonth target/hdp-probe.jar");
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

            // dataframe of Trips
            Dataset<Row> trips = getTripsDataset(spark, properties.get(BIXI_DATASET_PATH ) + "/trips/*");

            System.out.println("Average trip duration per day of month are:: ");
            getDayOfMonthAverages(trips).show(50);

            spark.stop();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Dataset<Row> getDayOfMonthAverages(Dataset<Row> trips) {
        Dataset<Row> tripWithTimestamp =
                trips.withColumn("start_date_1", trips.col("start_date").cast(DataTypes.TimestampType))
                        .withColumn("duration_sec", functions.col("duration_sec").cast(DataTypes.IntegerType));

        return tripWithTimestamp
                .withColumn("day",
                        functions.date_format(tripWithTimestamp.col("start_date_1"), "dd"))
                .groupBy("day")
                .agg(functions.avg("duration_sec").as("avg_trip_seconds"))
                .orderBy(asc("day"));
    }

    public static Dataset<Row> getTripsDataset(SparkSession spark, String path) {
        Dataset<Row> trips = spark.read()
                .format("csv").option("header", "true").load(path);

        System.out.println("Trips data sneak peek");
        trips.show(10);

        System.out.println("Schema of trip dataset");
        trips.printSchema();
        return trips.withColumn("duration_sec", trips.col("duration_sec").cast(DataTypes.IntegerType));
    }

}
