package com.gautam.mantra.spark;

import com.gautam.mantra.commons.Utilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

        spark.sparkContext().addFile("/etc/hbase/3.1.0.0-78/0/hbase-site.xml");

        Dataset<Row> dataset = generateDataSet(spark, Integer.parseInt(properties.get("sparkHBaseNumRecords")));
        dataset.show(10, false);
        writeDatasetToHBase(properties, dataset);
        spark.stop();
    }

    private static void writeDatasetToHBase(Map<String, String> properties, Dataset<Row> dataset) {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, properties.get("zkQuorum"));
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, properties.get("zkPort"));
        conf.set(HConstants.HBASE_DIR, properties.get("hbaseDataDir"));
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, properties.get("hbaseZnodeParent"));

        String catalog = "{\n" +
                "\"table\":{\"namespace\":\"default\", \"name\":\"" + properties.get("sparkHBaseTableName") + "\"},\n" +
                "\"rowkey\":\"key\",\n" +
                "\"columns\":{\n" +
                "\"rowkey\":{\"cf\":\"rowkey\", \"col\":\"key\", \"type\":\"string\"},\n" +
                "\"officeAddress\":{\"cf\":\"Office\", \"col\":\"Address\", \"type\":\"string\"},\n" +
                "\"officePhone\":{\"cf\":\"Office\", \"col\":\"Phone\", \"type\":\"string\"},\n" +
                "\"personalName\":{\"cf\":\"Personal\", \"col\":\"Name\", \"type\":\"string\"},\n" +
                "\"personalPhone\":{\"cf\":\"Personal\", \"col\":\"Phone\", \"type\":\"string\"}\n" +
                "}\n" +
                "}";

        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put(HBaseTableCatalog.tableCatalog(), catalog);
        optionsMap.put(HBaseTableCatalog.newTable(), "5");
        optionsMap.put(HConstants.ZOOKEEPER_QUORUM, properties.get("zkQuorum"));
        optionsMap.put(HConstants.ZOOKEEPER_CLIENT_PORT, properties.get("zkPort"));
        optionsMap.put(HConstants.HBASE_DIR, properties.get("hbaseDataDir"));
        optionsMap.put(HConstants.ZOOKEEPER_ZNODE_PARENT, properties.get("hbaseZnodeParent"));

        dataset.withColumnRenamed("rowKey", "rowkey").write()
                .options(optionsMap)
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .save();
    }

    /**
     * This method generates some event data
     * @param spark the spark session
     * @param numRows number of rows to be created
     * @return returns the spark dataframe
     */
    public static Dataset<Row> generateDataSet(SparkSession spark, int numRows){
        List<ContactRecord> dataList = new ArrayList<>();

        for (int i=1; i <= numRows; i++){
            dataList.add(new ContactRecord(String.valueOf(i), i + " Ellis St.",
                    "674-666-" + i, "John Jackson","230-555-" + i));
        }

        for (ContactRecord e: dataList) {
            Logger.getLogger("org").debug(e.toString());
        }

        return spark.createDataFrame(dataList, ContactRecord.class);
    }
}
