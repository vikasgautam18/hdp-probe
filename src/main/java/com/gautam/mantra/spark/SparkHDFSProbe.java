package com.gautam.mantra.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Timestamp;
import java.util.ArrayList;

public class SparkHDFSProbe {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder()
                .appName("spark-hdfs-test")
                .master("local[1]").getOrCreate();

        Dataset<Row> dataset = generateDataSet(spark, 100);
        dataset.show();

        dataset.write().csv("/user/vikgautammbb/spark-hdfs-test");
        spark.stop();
    }

    public static Dataset<Row> generateDataSet(SparkSession spark, int numRows){
        ArrayList<Event> dataList = new ArrayList<>();

        for (int i=1; i <= numRows; i++){
            dataList.add(new Event("event-" + i, new Timestamp(System.currentTimeMillis())));
        }

        for (Event e: dataList
             ) {
            System.out.println(e.toString());
        }

        return spark.createDataFrame(dataList, Event.class);
    }
}
