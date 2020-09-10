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

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

class SparkHDFSProbeTest {
    static SparkSession spark;
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    static Map<String, String> properties;
    public static final Yaml yaml = new Yaml();
    static final Utilities utilities = new Utilities();

    @BeforeAll
    static void setUp() {
       logger.info("starting up Spark session.. ");
       spark  = SparkSession.builder()
                .appName("TestSparkHDFS")
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
    void writeDatasetToHDFS() throws IOException {
        spark.sparkContext().hadoopConfiguration().forEach(stringStringEntry ->
            logger.info(stringStringEntry.getKey() + "--> " + stringStringEntry.getValue()));

        Dataset<Row> dataset = SparkHDFSProbe.generateDataSet(spark, Integer.parseInt(properties.get("sparkHDFSNumRecords")));
        dataset.show(10, false);
        // write to HDFS
        SparkHDFSProbe.writeDatasetToHDFS(properties, spark, dataset);

        Path path = Paths.get(properties.get("sparkHDFSFinalFile"));
        assert Files.lines(path).count() == Integer.parseInt(properties.get("sparkHDFSNumRecords")) + 1;

    }

    @Test
    void generateDataSet() {
        Dataset<Row> dataset = SparkHDFSProbe.generateDataSet(spark, Integer.parseInt(properties.get("sparkHDFSNumRecords")));
        dataset.show();
        dataset.printSchema();

        assert dataset.count() == Integer.parseInt(properties.get("sparkHDFSNumRecords"));
    }
}