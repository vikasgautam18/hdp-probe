package com.gautam.mantra.spark.extras;

import com.gautam.mantra.commons.Utilities;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
 * 2. How many products have been sold at least once?
 * 3. Which is the most ordered product?
 * 4. How many distinct products have been sold in each day?
 * 5. What is the average revenue of the orders?
 * 6. For each seller, what is the average % contribution of an order to the seller's daily quota?
 * 7. Who are the second most selling and the least selling persons (sellers) for each product?
 *    Who are those for product with `product_id = 0`
 * 8. Create a new column called "hashed_bill" defined as follows:
 *      - if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field,
 *          once for each 'A' (capital 'A') present in the text. E.g. if the bill text is 'nbAAnllA',
 *          you would apply hashing three times iteratively (only if the order number is even)
 *      - if the order_id is odd: apply SHA256 hashing to the bill text
 *    Finally, check if there are any duplicate on the new column
 */

public class ShopAnalysisPart1 {

    private static final String CLUSTER_CONFIG = "spark.probe.cluster.yml";
    public static final Yaml yaml = new Yaml();

    public static void main(String[] args) {
        if(args.length != 0){
            System.out.println("USAGE: spark-submit --driver-java-options \"-Dspark.probe.cluster.yml=conf/cluster-conf.yml\" " +
                    "--class com.gautam.mantra.spark.extras.ShopAnalysisPart1 target/hdp-probe.jar");
        }
        Logger.getRootLogger().setLevel(Level.ERROR);
        InputStream inputStream;
        try {
            inputStream = new FileInputStream(
                    new File(System.getProperty(CLUSTER_CONFIG)));
            Utilities utilities = new Utilities();

            Map<String, String> properties = yaml.load(inputStream);
            // print all loaded properties to console
            utilities.printProperties(properties);

            SalesAnalysis salesAnalysis = new SalesAnalysis(properties);
            salesAnalysis.process();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
