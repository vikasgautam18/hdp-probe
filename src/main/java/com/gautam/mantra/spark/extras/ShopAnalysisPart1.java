package com.gautam.mantra.spark.extras;

import com.gautam.mantra.commons.Product;
import com.gautam.mantra.commons.Sales;
import com.gautam.mantra.commons.Seller;
import com.gautam.mantra.commons.Utilities;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

/**
 * Generate dataset using below:
 * https://gist.github.com/aialenti/cfd4e213ebf2ef6e20b195c8fb45382c
 *
 * Goal:
 * 1. Find out how many orders, how many products and how many sellers are in the data.
 * 2. How many products have been sold at least once? Which is the product contained in more orders?
 */

public class ShopAnalysisPart1 {

    private static final String CLUSTER_CONFIG = "spark.probe.cluster.yml";
    private static final String APP_NAME = "shop.data.application";
    public static final Yaml yaml = new Yaml();
    public static final String SALES_IN_PATH = "sales.dataset.hdfs.path";
    public static final String SELLER_IN_PATH = "seller.dataset.hdfs.path";
    public static final String PRODUCT_IN_PATH = "product.dataset.hdfs.path";

    public static void main(String[] args) {
        if(args.length != 0){
            System.out.println("USAGE: spark-submit --driver-java-options \"-Dspark.probe.cluster.yml=conf/cluster-conf.yml\" " +
                    "--class com.gautam.mantra.spark.extras.ShopAnalysisPart1 target/hdp-probe.jar");
        }
        Logger.getRootLogger().setLevel(Level.ERROR);

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

            // 1. Find out how many orders, how many products and how many sellers are in the dataset
            Dataset<Sales> sales = spark.read()
                    .parquet(properties.get(SALES_IN_PATH)).as(Encoders.bean(Sales.class));
            System.out.printf("The count of sales dataset is :: %s%n", sales.count());

            Dataset<Seller> sellers = spark.read()
                    .parquet(properties.get(SELLER_IN_PATH)).as(Encoders.bean(Seller.class));
            System.out.printf("The count of sellers dataset is :: %s%n", sellers.count());

            Dataset<Product> products = spark.read()
                    .parquet(properties.get(PRODUCT_IN_PATH)).as(Encoders.bean(Product.class));
            System.out.printf("The count of product dataset is :: %s%n", products.count());

            System.out.println(sales.select("product_id").distinct().count());

            spark.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
