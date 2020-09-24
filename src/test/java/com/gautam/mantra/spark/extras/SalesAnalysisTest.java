package com.gautam.mantra.spark.extras;

import com.gautam.mantra.commons.Product;
import com.gautam.mantra.commons.Utilities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
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

class SalesAnalysisTest {

    static SparkSession spark;
    private static final Logger logger =
            LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    static Map<String, String> properties;
    public static final Yaml yaml = new Yaml();
    static final Utilities utilities = new Utilities();

    @BeforeAll
    static void setUp() {
        logger.info("starting up Spark session.. ");
        spark  = SparkSession.builder()
                .appName("SalesAnalysisTest")
                .master("local")
                .config("job.local.dir", "/tmp/")
                .getOrCreate();

        logger.info("started Spark session.. ");

        InputStream inputStream =
                MethodHandles.lookup().lookupClass().getClassLoader().getResourceAsStream("cluster-conf.yml");
        properties = yaml.load(inputStream);
        utilities.printProperties(properties);
    }

    @AfterAll
    static void tearDown() {
        spark.stop();
        logger.info("stopped Spark session.. ");
    }

    @Test
    void getProductsSoldAtleastOnce() {
    }

    @Test
    void getMostPopularProduct() {
    }

    @Test
    void process() {
        Dataset<Row> csvData = spark
                .read()
                .option("header", "true")
                .csv("./src/test/resources/shop-data/products.csv");

        csvData.show();

        csvData.write().parquet("./src/test/resources/shop-data/products.parquet");

        Dataset<Product> parquetData = spark
                .read()
                .option("spark.sql.parquet.mergeSchema", "true")
                .parquet("./src/test/resources/shop-data/products.parquet")
                .as(Encoders.bean(Product.class));

        parquetData.show();

    }

    @Test
    void readProductDataset() {
    }

    @Test
    void readSellerDataset() {
    }

    @Test
    void readSalesDataset() {
    }
}