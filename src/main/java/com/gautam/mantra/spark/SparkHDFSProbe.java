package com.gautam.mantra.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;

public class SparkHDFSProbe {

    public static void main(String[] args) throws URISyntaxException, IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder()
                .appName("spark-hdfs-test")
                .master("local[1]").getOrCreate();

        Dataset<Row> dataset = generateDataSet(spark, 100);
        dataset.show();

        dataset.coalesce(1).write()
                .format("csv").option("header", "false")
                .mode(SaveMode.Overwrite)
                .save("/user/vikgautammbb/spark-hdfs-test");

        Configuration config = spark.sparkContext().hadoopConfiguration();
        FileSystem fs = FileSystem.get(new URI("/user/vikgautammbb/spark-hdfs-test"), config);
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/user/vikgautammbb/spark-hdfs-test"),
                false);

        Path outPath = null;
        while(files.hasNext()){
            Path path = files.next().getPath();
            System.out.println(files.next().getPath().getName());
            if(path.getName().startsWith("part-00000"))
                outPath = path;
        }

        //String srcPath = "/user/vikgautammbb/spark-hdfs-test/";
        String dstPath = "/user/vikgautammbb/spark-hdfs-test.csv";

        if(outPath != null){
            System.out.println(outPath);
            FileUtil.copy(fs, outPath, fs, new Path(dstPath), true, config);
            FileUtil.fullyDelete(new File("/user/vikgautammbb/spark-hdfs-test"));
        }


        spark.stop();
    }

    public static Dataset<Row> generateDataSet(SparkSession spark, int numRows){
        ArrayList<Event> dataList = new ArrayList<>();

        for (int i=1; i <= numRows; i++){
            dataList.add(new Event("event-" + i, new Timestamp(System.currentTimeMillis())));
        }

        for (Event e: dataList) {
            System.out.println(e.toString());
        }

        return spark.createDataFrame(dataList, Event.class);
    }
}
