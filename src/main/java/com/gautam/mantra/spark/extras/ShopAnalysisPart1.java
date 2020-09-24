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
