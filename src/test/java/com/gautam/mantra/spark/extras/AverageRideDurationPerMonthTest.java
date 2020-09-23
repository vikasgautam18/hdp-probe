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
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;

class AverageRideDurationPerMonthTest implements Serializable {
    static SparkSession spark;
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    static Map<String, String> properties;
    public static final Yaml yaml = new Yaml();
    static final Utilities utilities = new Utilities();

    @BeforeAll
    static void setUp() {
        logger.info("starting up Spark session.. ");
        spark  = SparkSession.builder()
                .appName("AverageRideDurationPerMonth")
                .master("local")
                .config("job.local.dir", "/tmp/")
                .getOrCreate();

        logger.info("started Spark session.. ");

        InputStream inputStream = MethodHandles
                .lookup()
                .lookupClass()
                .getClassLoader()
                .getResourceAsStream("cluster-conf.yml");
        properties = yaml.load(inputStream);
        utilities.printProperties(properties);
    }

    @AfterAll
    static void tearDown() {
        spark.stop();
        logger.info("stopped Spark session.. ");
    }

    @Test
    void getTripsDatasetTest(){
        Dataset<Row> trips = AverageRideDurationPerMonth.getTripsDataset(spark,
                properties.get("bixi.dataset.path" ) + "/trips/*");

        assert trips.count() == 50;
    }

    @Test
    void getAverage(){
        Dataset<Row> trips = AverageRideDurationPerMonth.getTripsDataset(spark,
                properties.get("bixi.dataset.path" ) + "/trips/*");

        Dataset<Row> result = AverageRideDurationPerMonth.getMonthlyAverages(trips);
        result.show();
        assert result.collectAsList().get(0).getAs("month").toString().equals("07");
        assert result.collectAsList().get(0).getAs("avg_trip_seconds").toString().equals("828.62");

    }
}