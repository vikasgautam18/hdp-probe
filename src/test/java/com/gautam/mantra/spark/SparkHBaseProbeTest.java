package com.gautam.mantra.spark;

import com.gautam.mantra.commons.Utilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Map;

class SparkHBaseProbeTest {
    private static HBaseTestingUtility utility;
    static SparkSession spark;
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    static Map<String, String> properties;
    public static final Yaml yaml = new Yaml();
    static final Utilities utilities = new Utilities();

    @BeforeAll
    static void setUp() {
        InputStream inputStream = MethodHandles.lookup()
                .lookupClass().getClassLoader()
                .getResourceAsStream("cluster-conf.yml");

        properties = yaml.load(inputStream);
        utilities.printProperties(properties);

        Configuration conf = HBaseConfiguration.create();
        conf.set("test.hbase.zookeeper.property.clientPort", properties.get("zkPort"));
        conf.set("test.hbase.rootdir", properties.get("hbaseDataDir"));


        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        conf.forEach(System.out::println);
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

        utility = new HBaseTestingUtility(conf);

        try {
            logger.info("*** Starting HBase Mini Cluster ***");
            utility.startMiniCluster();

            HBaseAdmin.available(utility.getConfiguration());
            logger.info("*** HBase Mini Cluster Successfully started ***");

        } catch (Exception e) {
            e.printStackTrace();
        }

        logger.info("starting up Spark session.. ");
        spark  = SparkSession.builder()
                .appName("TestSparkHBase")
                .master("local")
                .getOrCreate();

        logger.info("started Spark session.. ");
        logger.info("*** Setting up Hbase testing utility ***");

    }

    @AfterAll
    static void tearDown() {
        logger.info("*** Shutting down Hbase Mini Cluster ***");
        try {
            utility.shutdownMiniCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
        spark.stop();
        logger.info("stopped Spark session.. ");
    }

    @Test
    void writeDatasetToSparkHbase() {
        utility.getConfiguration().forEach(System.out::println);
        Dataset<Row> dataset = SparkHBaseProbe.generateDataSet(spark, Integer.parseInt(properties.get("sparkHBaseNumRecords")));
        TableName tableName = TableName.valueOf(properties.get("sparkHBaseTableName"));

        try {
            utility.getAdmin().createTable(TableDescriptorBuilder
                    .newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("Office"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("Personal"))
                    .build());

            SparkHBaseProbe.writeDatasetToHBase(properties, dataset);

            assert utility.countRows(tableName) == Integer.parseInt(properties.get("sparkHBaseNumRecords"));
        } catch (IOException e) {
            e.printStackTrace();
            assert false;
        }
    }

    @Test
    void generateDataSet() {
        Dataset<Row> dataset = SparkHBaseProbe.generateDataSet(spark, Integer.parseInt(properties.get("sparkHBaseNumRecords")));
        dataset.show();
        dataset.printSchema();

        assert dataset.count() == Integer.parseInt(properties.get("sparkHBaseNumRecords"));
    }
}