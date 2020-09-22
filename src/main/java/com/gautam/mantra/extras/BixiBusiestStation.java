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
import java.lang.invoke.MethodHandles;
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
        Logger.getLogger("org").setLevel(Level.ERROR);

        Logger logger = Logger.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

        logger.info("Begin main method... ");
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
                    .load(BIXI_DATASET_PATH + "/" + BIXI_STATION );
            stations.show(10);

            // dataframe of Trips
            Dataset<Row> trips = spark.read()
                    .format("csv").option("header", "true").load(BIXI_DATASET_PATH + "/trips/*");

            trips.show(10);

            // group trips by station code
            trips.printSchema();

            trips.groupBy("start_station_code")
                    .count().sort(desc("count")).show();

            // find station code with highest number of trips

            // lookup station name with station code

            // print result

            spark.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
