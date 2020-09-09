package com.gautam.mantra.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class SparkHDFSProbe {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder()
                .appName("HousePriceSolution")
                .master("local[1]").getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("event_id", DataTypes.StringType, false),
                DataTypes.createStructField("event_ts", TimestampType, false)
        });

        Dataset<Row> dataset = spark.read().schema(schema).csv(spark.emptyDataset(Encoders.STRING()));

        dataset.printSchema();
        dataset.show();

        spark.stop();
    }
}
