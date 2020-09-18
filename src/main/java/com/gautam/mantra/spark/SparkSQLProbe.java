package com.gautam.mantra.spark;

import com.gautam.mantra.commons.Event;
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

/**
 * A simple spark job to generate some event data and write it to spark sql
 * Also, exports the data from spark sql to file
 */
public class SparkSQLProbe {

    public static final Yaml yaml = new Yaml();
    public static final String TABLE_SEPARATOR = ".";

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
                .appName(properties.get("sparkHiveAppName"))
                .enableHiveSupport()
                .config("hive.metastore.schema.verification", "false")
                .getOrCreate();

        Dataset<Row> dataset = generateDataSet(spark, Integer.parseInt(properties.get("sparkHiveNumRecords")));
        dataset.show(10, false);
        // write to HDFS
        writeDatasetToSparkSQL(properties, spark, dataset);
        exportDataToHDFS(spark, properties.get("sparkHiveDB") +
                TABLE_SEPARATOR + properties.get("sparkHiveTable"), properties);

        spark.stop();
    }

    /**
     * This method writes a dataset to spark-sql
     * @param properties the cluster configuration
     * @param spark the spark session
     * @param dataset the dataset to be written to spark-sql
     */
    public static void writeDatasetToSparkSQL(Map<String, String> properties, SparkSession spark, Dataset<Row> dataset) {

        // create database if not exists
        spark.sql("create database if not exists " + properties.get("sparkHiveDB"));

        // tablename = database.table
        String finalTableName = properties.get("sparkHiveDB") + TABLE_SEPARATOR + properties.get("sparkHiveTable");

        // write dataset in  overwrite mode
        dataset.write().mode(SaveMode.Overwrite).saveAsTable(finalTableName);
    }

    /**
     * This method reads all the daya from a given spark-sql table and exports it to a file in HDFS
     * @param spark the spark session
     * @param finalTableName the table name = database name + table name
     * @param properties the cluster configuration
     */
    private static void exportDataToHDFS(SparkSession spark, String finalTableName, Map<String, String> properties) {
        spark.sql("select * from " + finalTableName)
                .coalesce(1)
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