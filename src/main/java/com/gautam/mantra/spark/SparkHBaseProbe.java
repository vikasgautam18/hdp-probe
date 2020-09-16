package com.gautam.mantra.spark;

import com.gautam.mantra.commons.Utilities;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.*;

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
        writeDatasetToHBase(properties, spark, dataset);
        spark.stop();
    }

    private static void writeDatasetToHBase(Map<String, String> properties, SparkSession spark, Dataset<Row> dataset) {
//        Configuration conf = HBaseConfiguration.create();
//        conf.set(HConstants.ZOOKEEPER_QUORUM, properties.get("zkQuorum"));
//        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, properties.get("zkPort"));
//        conf.set(HConstants.HBASE_DIR, properties.get("hbaseDataDir"));
//        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, properties.get("hbaseZnodeParent"));

        //String newLine = System.getProperty("line.separator");

        /*String catalog = new StringBuilder().append("{")
                .append(newLine)
                .append("\"table\":{\"namespace\":\"default\", \"name\":\"events\"},").append(newLine)
                .append("\"rowkey\":\"eventId\",").append(newLine)
                .append("\"columns\":{").append(newLine)
                .append("\"rowkey\":{\"cf\":\"rowkey\", \"col\":\"eventId\", \"type\":\"string\"},").append(newLine)
                .append("\"eventTs\":{\"cf\":\"cf\", \"col\":\"eventTs\", \"type\":\"timestamp\"},").append(newLine)
                .append("}").append(newLine)
                .append("}")
                .toString().trim();*/

        String catalog = "{\n" +
                "\"table\":{\"namespace\":\"default\", \"name\":\"Contacts\"},\n" +
                "\"rowkey\":\"key\",\n" +
                "\"columns\":{\n" +
                "\"rowkey\":{\"cf\":\"rowkey\", \"col\":\"key\", \"type\":\"string\"},\n" +
                "\"officeAddress\":{\"cf\":\"Office\", \"col\":\"Address\", \"type\":\"string\"},\n" +
                "\"officePhone\":{\"cf\":\"Office\", \"col\":\"Phone\", \"type\":\"string\"},\n" +
                "\"personalName\":{\"cf\":\"Personal\", \"col\":\"Name\", \"type\":\"string\"},\n" +
                "\"personalPhone\":{\"cf\":\"Personal\", \"col\":\"Phone\", \"type\":\"string\"}\n" +
                "}\n" +
                "}";

        System.out.println(catalog);

        Map<String, String> tempMap = new HashMap<>();
        tempMap.put(HBaseTableCatalog.tableCatalog(), catalog);
        tempMap.put(HBaseTableCatalog.newTable(), "5");

        ContactRecord contactRecord = new ContactRecord("16895", "45 Ellis St.",
                "674-555-0110", "John Jackson","230-555-0194");

        List<ContactRecord> contactRecordList = Arrays.asList(contactRecord);
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext().conf());

        JavaRDD<ContactRecord> rdd = sc.parallelize(contactRecordList);

        spark.createDataFrame(rdd, ContactRecord.class)
        .write()
        .options(tempMap)
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
        ArrayList<Event> dataList = new ArrayList<>();

        for (int i=1; i <= numRows; i++){
            dataList.add(new Event("event-" + i, new Timestamp(System.currentTimeMillis())));
        }

        for (Event e: dataList) {
            Logger.getLogger("org").debug(e.toString());
        }

        return spark.createDataFrame(dataList, Event.class);
    }

    /*
spark-shell --jars $(echo /usr/hdp/3.1.0.0-78/hbase/lib/*.jar | tr ' ' ',')
case class ContactRecord(
    rowkey: String,
    officeAddress: String,
    officePhone: String,
    personalName: String,
    personalPhone: String
    )

import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.{SparkConf, SparkContext}
import spark.sqlContext.implicits._

def catalog = s"""{
    |"table":{"namespace":"default", "name":"Contacts"},
    |"rowkey":"key",
    |"columns":{
    |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
    |"officeAddress":{"cf":"Office", "col":"Address", "type":"string"},
    |"officePhone":{"cf":"Office", "col":"Phone", "type":"string"},
    |"personalName":{"cf":"Personal", "col":"Name", "type":"string"},
    |"personalPhone":{"cf":"Personal", "col":"Phone", "type":"string"}
    |}
|}""".stripMargin

val newContact = ContactRecord("16891", "40 Ellis St.", "674-555-0110", "John Jackson","230-555-0194")

var newData = new Array[ContactRecord](1)
newData(0) = newContact

sc.parallelize(newData)
.toDF
.write
.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
.format("org.apache.spark.sql.execution.datasources.hbase")
.save()
     */
}
