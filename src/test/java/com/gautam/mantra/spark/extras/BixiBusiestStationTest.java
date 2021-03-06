package com.gautam.mantra.spark.extras;

import com.gautam.mantra.commons.Utilities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static org.apache.spark.sql.functions.desc;

class BixiBusiestStationTest {
    static SparkSession spark;
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    static Map<String, String> properties;
    public static final Yaml yaml = new Yaml();
    static final Utilities utilities = new Utilities();

    @BeforeAll
    static void setUp() {
        logger.info("starting up Spark session.. ");
        spark  = SparkSession.builder()
                .appName("BixiBusiestStationTest")
                .master("local")
                .config("job.local.dir", "/tmp/")
                .getOrCreate();

        logger.info("started Spark session.. ");

        InputStream inputStream = MethodHandles.lookup().lookupClass().getClassLoader().getResourceAsStream("cluster-conf.yml");
        properties = yaml.load(inputStream);
        utilities.printProperties(properties);
    }

    @AfterAll
    static void tearDown() {
        spark.stop();
        logger.info("stopped Spark session.. ");
    }


    @Test
    void getStationDatasetTest(){
        Dataset<Row> stations = BixiBusiestStation.getStationDataset(spark,
                properties.get("bixi.dataset.path") + "/" + properties.get("bixi.station.file.name"));
        assert stations.count() == 7;
        assert stations.orderBy(desc("Code")).first().getAs("Code").equals("5005");
    }

    @Test
    void getTripsDatasetTest(){
        Dataset<Row> trips = BixiBusiestStation.getTripsDataset(spark,
                properties.get("bixi.dataset.path" ) + "/trips/*");

        assert trips.count() == 50;
    }

    @Test
    void findBusiestStationTest(){
        Dataset<Row> stations = BixiBusiestStation.getStationDataset(spark,
                properties.get("bixi.dataset.path") + "/" + properties.get("bixi.station.file.name"));

        Dataset<Row> trips = BixiBusiestStation.getTripsDataset(spark,
                properties.get("bixi.dataset.path" ) + "/trips/*");

        assert BixiBusiestStation.findBusiestStation(stations, trips).equals("St-Charles Main");
    }
}