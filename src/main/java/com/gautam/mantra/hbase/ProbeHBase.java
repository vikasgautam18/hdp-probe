package com.gautam.mantra.hbase;

import com.gautam.mantra.commons.ProbeDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;

public class ProbeHBase implements ProbeDatabase {

    public static Connection connection;

    public ProbeHBase(Configuration conf){
        try {
            connection = ConnectionFactory.createConnection(conf);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Boolean createDatabase(String database) {

        return null;
    }

    @Override
    public Boolean deleteDatabase(String database) {
        return null;
    }

    @Override
    public Boolean createTable(String database, String table) {
        return null;
    }

    @Override
    public Boolean readTable(String database, String table) {
        return null;
    }

    @Override
    public Boolean deleteTable(String database, String table) {
        return null;
    }

    @Override
    public Boolean isReachable(Map<String, String> properties) {
        return !connection.isClosed();
    }
}
