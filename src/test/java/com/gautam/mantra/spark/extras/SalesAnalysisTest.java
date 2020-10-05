package com.gautam.mantra.spark.extras;

import com.gautam.mantra.commons.Product;
import com.gautam.mantra.commons.Sales;
import com.gautam.mantra.commons.Seller;
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
    static Dataset<Product> productDataset;
    static Dataset<Sales> salesDataset;
    static Dataset<Seller> sellerDataset;

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

        productDataset = readProductDataset();
        sellerDataset = readSellerDataset();
        salesDataset = readSalesDataset();
    }

    @AfterAll
    static void tearDown() {
        spark.stop();
        logger.info("stopped Spark session.. ");
    }

    @Test
    void process() {

        SalesAnalysis salesAnalysis = new SalesAnalysis(properties);

        productDataset.show();
        salesDataset.show();
        sellerDataset.show();

        long result = salesAnalysis.getProductsSoldAtleastOnce(salesDataset);
        System.out.println("The number of products which have been sold atleast once:: "
                + result);

        assert productDataset.count() == 14;
        assert salesDataset.count() == 13;
        assert sellerDataset.count() == 10;
        assert result == 3;

        Row row = salesAnalysis.getMostPopularProduct(salesDataset);
        System.out.printf("The product with Id '%s' is the most popular one with over %s items sold%n",
                row.getAs("product_id"), row.getAs("count_sold"));

        assert  row.getAs("product_id").equals("0");
        assert  Long.parseLong(row.getAs("count_sold").toString()) == 11L;

        Dataset<Row> distinctProductsPerDay = salesAnalysis.getDistinctProductsSoldPerDay(salesDataset);
        distinctProductsPerDay.show();

        assert distinctProductsPerDay.count() == 7;

    }


    private static Dataset<Product> readProductDataset() {
        return spark.read()
            .option("header", "true")
            .csv("./src/test/resources/shop-data/products.csv")
            .as(Encoders.bean(Product.class));
    }


    private static Dataset<Seller> readSellerDataset() {
        return spark.read()
            .option("header", "true")
            .csv("./src/test/resources/shop-data/sellers.csv")
            .as(Encoders.bean(Seller.class));
    }


    private static Dataset<Sales> readSalesDataset() {
        return spark.read()
                .option("header", "true")
                .csv("./src/test/resources/shop-data/sales.csv")
                .as(Encoders.bean(Sales.class));
    }
}