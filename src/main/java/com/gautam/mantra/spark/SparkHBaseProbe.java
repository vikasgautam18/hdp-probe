package com.gautam.mantra.spark;

import com.gautam.mantra.commons.Utilities;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;

/**
 * A simple spark job to generate some event data and write it to spark sql
 * Also, exports the data from spark sql to file
 */
public class SparkHBaseProbe {

    public static final Yaml yaml = new Yaml();

    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        InputStream inputStream = new FileInputStream(
                new File(System.getProperty("spark.probe.cluster.yml")));
        Utilities utilities = new Utilities();

        Map<String, String> properties = yaml.load(inputStream);
        // print all loaded properties to console
        utilities.printProperties(properties);

        System.out.println("**********************************************************************************");

        SparkSession spark = SparkSession.builder()
                .appName(properties.get("sparkHBaseAppName"))
                .getOrCreate();


        Dataset<Row> dataset = generateDataSet(spark, Integer.parseInt(properties.get("sparkHBaseNumRecords")));
        dataset.show(10, false);
        writeDatasetToHBase(properties, dataset);
        spark.stop();
    }

    private static void writeDatasetToHBase(Map<String, String> properties, Dataset<Row> dataset) {
        dataset.write().format("org.apache.hadoop.hbase.spark")
                .option("hbase.columns.mapping",
                        "eventId STRING :key, eventTs TIMESTAMP cf:eventTs ")
                .option("hbase.table", properties.get("sparkHBaseTableName"))
                .option("hbase.spark.use.hbasecontext", false)
                .save();
    }

    /**
     * This method generates some event data
     * @param spark the spark session
     * @param numRows number of rows to be created
     * @return returns the spark dataframe
     */
    public static Dataset<Row> generateDataSet(SparkSession spark, int numRows){
        ArrayList<Event> dataList = new ArrayList<>();

        for (int i=1; i <= numRows; i++){
            dataList.add(new Event("event-" + i, new Timestamp(System.currentTimeMillis())));
        }

        for (Event e: dataList) {
            Logger.getLogger("org").debug(e.toString());
        }

        return spark.createDataFrame(dataList, Event.class);
    }
}
