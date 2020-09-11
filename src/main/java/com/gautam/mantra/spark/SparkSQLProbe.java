package com.gautam.mantra.spark;

import com.gautam.mantra.commons.Utilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;

public class SparkSQLProbe {

    public static final Yaml yaml = new Yaml();
    public static final String TABLE_SEPARATOR = ".";

    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        InputStream inputStream = new FileInputStream(
                new File(System.getProperty("spark2hive.cluster.yml")));
        Utilities utilities = new Utilities();

        Map<String, String> properties = yaml.load(inputStream);
        // print all loaded properties to console
        utilities.printProperties(properties);

        System.out.println("**********************************************************************************");

        SparkSession spark = SparkSession.builder()
                .appName(properties.get("sparkHiveAppName"))
                .enableHiveSupport()
                .config("hive.metastore.schema.verification", "false")
                .getOrCreate();

        Dataset<Row> dataset = generateDataSet(spark, Integer.parseInt(properties.get("sparkHiveNumRecords")));
        dataset.show(10, false);
        // write to HDFS
        writeDatasetToHive(properties, spark, dataset);
        spark.stop();
    }

    public static void writeDatasetToHive(Map<String, String> properties, SparkSession spark, Dataset<Row> dataset) {

        // create database if not exists
        spark.sql("create database if not exists " + properties.get("sparkHiveDB"));

        // tablename = database.table
        String finalTableName = properties.get("sparkHiveDB") + TABLE_SEPARATOR + properties.get("sparkHiveTable");

        // write dataset in  overwrite mode
        dataset.write().mode(SaveMode.Overwrite).saveAsTable(finalTableName);

        exportDataToHDFS(spark, finalTableName, properties);
    }

    private static void exportDataToHDFS(SparkSession spark, String finalTableName, Map<String, String> properties) {
        spark.sql("select * from " + finalTableName)
                .write().mode(SaveMode.Overwrite)
                .csv(properties.get("sparkSQLExportFolder"));

        try {
            Configuration config = spark.sparkContext().hadoopConfiguration();
            FileSystem fs = FileSystem.get(config);
            RemoteIterator<LocatedFileStatus> files;
            files = fs.listFiles(new Path(properties.get("sparkSQLExportFolder")), true);
            Path outPath = null;
            while(files.hasNext()){
                Path path = files.next().getPath();
                Logger.getLogger("org").info("found file:: "+ path.getName());
                if(path.getName().startsWith("part-00000"))
                    outPath = path;
            }

            String dstPath = properties.get("sparkSQLExportFile");
            Logger.getLogger("org").debug("outpath::" + outPath);

            if(outPath != null){
                FileUtil.copy(fs, outPath, fs, new Path(dstPath), false, config);
                Logger.getLogger("org").info("Output file written to HDFS at ::" + dstPath);
            }

            fs.delete(new Path(properties.get("sparkSQLExportFolder")), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
