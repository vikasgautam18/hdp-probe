package com.gautam.mantra.spark;

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

class SparkHiveProbeTest {

    static SparkSession spark;
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    static Map<String, String> properties;
    public static final Yaml yaml = new Yaml();
    static final Utilities utilities = new Utilities();

    @BeforeAll
    static void setUp() {
        logger.info("starting up Spark session.. ");
        spark  = SparkSession.builder()
                .appName("TestSparkHive")
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
    void writeDatasetToSparkSQL() {
        Dataset<Row> dataset = SparkHiveProbe.generateDataSet(spark, Integer.parseInt(properties.get("sparkHiveNumRecords")));
        dataset.show(10, false);
        // write to HDFS
        SparkHiveProbe.writeDatasetToHive(properties, spark, dataset);
        String finalTableName = properties.get("sparkHiveDB") + "." + properties.get("sparkHiveTable");
        assert spark.sql("select * from " + finalTableName).count() == Integer.parseInt(properties.get("sparkHiveNumRecords"));
    }

    @Test
    void generateDataSet() {
        Dataset<Row> dataset = SparkHiveProbe.generateDataSet(spark, Integer.parseInt(properties.get("sparkHiveNumRecords")));
        dataset.show();
        dataset.printSchema();

        assert dataset.count() == Integer.parseInt(properties.get("sparkHiveNumRecords"));
    }
}