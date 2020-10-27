package com.gautam.mantra.commons;

public class ClusterStatusResult {

    private boolean hdfsStatus;
    private boolean hbaseStatus;
    private boolean hiveStatus;
    private boolean kafkaStatus;
    private boolean oozieStatus;
    private boolean sparkStatus;
    private boolean zeppelinStatus;
    private boolean zookeeperStatus;


    public ClusterStatusResult(boolean hdfsStatus, boolean hbaseStatus,
                               boolean hiveStatus, boolean kafkaStatus,
                               boolean oozieStatus, boolean sparkStatus,
                               boolean zeppelinStatus, boolean zookeeperStatus) {
        this.hdfsStatus = hdfsStatus;
        this.hbaseStatus = hbaseStatus;
        this.hiveStatus = hiveStatus;
        this.kafkaStatus = kafkaStatus;
        this.oozieStatus = oozieStatus;
        this.sparkStatus = sparkStatus;
        this.zeppelinStatus = zeppelinStatus;
        this.zookeeperStatus = zookeeperStatus;
    }

    public ClusterStatusResult() {

    }

    public boolean isHdfsStatus() {
        return hdfsStatus;
    }

    public void setHdfsStatus(boolean hdfsStatus) {
        this.hdfsStatus = hdfsStatus;
    }

    public boolean isHbaseStatus() {
        return hbaseStatus;
    }

    public void setHbaseStatus(boolean hbaseStatus) {
        this.hbaseStatus = hbaseStatus;
    }

    public boolean isHiveStatus() {
        return hiveStatus;
    }

    public void setHiveStatus(boolean hiveStatus) {
        this.hiveStatus = hiveStatus;
    }

    public boolean isKafkaStatus() {
        return kafkaStatus;
    }

    public void setKafkaStatus(boolean kafkaStatus) {
        this.kafkaStatus = kafkaStatus;
    }

    public boolean isOozieStatus() {
        return oozieStatus;
    }

    public void setOozieStatus(boolean oozieStatus) {
        this.oozieStatus = oozieStatus;
    }

    public boolean isSparkStatus() {
        return sparkStatus;
    }

    public void setSparkStatus(boolean sparkStatus) {
        this.sparkStatus = sparkStatus;
    }

    public boolean isZeppelinStatus() {
        return zeppelinStatus;
    }

    public void setZeppelinStatus(boolean zeppelinStatus) {
        this.zeppelinStatus = zeppelinStatus;
    }

    public boolean isZookeeperStatus() {
        return zookeeperStatus;
    }

    public void setZookeeperStatus(boolean zookeeperStatus) {
        this.zookeeperStatus = zookeeperStatus;
    }

    @Override
    public String toString() {
        return "**************** RESULT ****************" + System.lineSeparator() +
                "HDFS       ========     " + mapResult(hdfsStatus) + System.lineSeparator() +
                "HBASE      ========     " + mapResult(hbaseStatus) + System.lineSeparator() +
                "HIVE       ========     " + mapResult(hiveStatus) + System.lineSeparator() +
                "KAFKA      ========     " + mapResult(kafkaStatus) + System.lineSeparator() +
                "OOZIE      ========     " + mapResult(oozieStatus) + System.lineSeparator() +
                "SPARK      ========     " + mapResult(sparkStatus) + System.lineSeparator() +
                "ZEPPELIN   ========     " + mapResult(zeppelinStatus) + System.lineSeparator() +
                "ZOOKEEPER  ========     " + mapResult(zookeeperStatus) + System.lineSeparator() +
                "***************************************" ;
    }

    public String mapResult(boolean result){
        return result ? "WORKING" : "NOT WORKING";
    }
}
