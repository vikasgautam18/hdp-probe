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
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.desc;

class Top10LongestRidesTest implements Serializable {
    static SparkSession spark;
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    static Map<String, String> properties;
    public static final Yaml yaml = new Yaml();
    static final Utilities utilities = new Utilities();
    public Map<String, String> stationMap;

    @BeforeAll
    static void setUp() {
        logger.info("starting up Spark session.. ");
        spark  = SparkSession.builder()
                .appName("Top10LongestRides")
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
    void getStationDatasetTest(){
        Dataset<Row> stations = Top10LongestRides.getStationDataset(spark,
                properties.get("bixi.dataset.path") + "/" + properties.get("bixi.station.file.name"));
        assert stations.count() == 7;
        assert stations.orderBy(desc("Code")).first().getAs("Code").equals("5005");
    }

    @Test
    void getTripsDatasetTest(){
        Dataset<Row> trips = Top10LongestRides.getTripsDataset(spark,
                properties.get("bixi.dataset.path" ) + "/trips/*");

        assert trips.count() == 50;
    }

    @Test
    void getTop10TripsTest(){
        Dataset<Row> stations = Top10LongestRides.getStationDataset(spark,
                properties.get("bixi.dataset.path") + "/" + properties.get("bixi.station.file.name"));

        Top10LongestRides.populateStationMap(stations);

        Dataset<Row> trips = Top10LongestRides.getTripsDataset(spark,
                properties.get("bixi.dataset.path" ) + "/trips/*");

        List<Row> longestTrips = Top10LongestRides.getTop10LongestTrips(trips, spark);

        longestTrips.forEach(Top10LongestRides::printTrip);

        assert longestTrips.size() == 10;
        assert longestTrips.get(0).getAs("start_station_name").equals("St-Charles / Grant");
        assert longestTrips.get(0).getAs("end_station_name").equals("St-Charles Main");
        assert longestTrips.get(0).getAs("duration_sec").equals("964");

    }


}