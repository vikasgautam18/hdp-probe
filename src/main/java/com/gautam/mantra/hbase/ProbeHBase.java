package com.gautam.mantra.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProbeHBase {

    public static Connection connection;
    private static Logger logger = LoggerFactory.getLogger(ProbeHBase.class.getName());
    Admin admin;

    public ProbeHBase(Configuration conf){
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Boolean createNameSpace(String namespace) {
        try {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
            return existsNameSpace(namespace);

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean existsNameSpace(String namespace){
        try {
            NamespaceDescriptor ns = NamespaceDescriptor.create(namespace).build();
            NamespaceDescriptor[] list = admin.listNamespaceDescriptors();
            boolean exists = false;
            for (NamespaceDescriptor nsd : list) {
                if (nsd.getName().equals(ns.getName())) {
                    exists = true;
                    break;
                }
            }

            return exists;

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Boolean deleteNameSpace(String namespace) {
        try {
            if(admin.listTableDescriptorsByNamespace(namespace.getBytes()).size() > 0){
                for (TableDescriptor td: admin.listTableDescriptorsByNamespace(namespace.getBytes())
                     ) {
                    deleteTable(td);
                }
            }

            admin.deleteNamespace(namespace);
            return !existsNameSpace(namespace);

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    public boolean createTable(String namespace, String table, String cf) {
        try {
            admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace + ":" + table))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf))
                    .build());

            return admin.tableExists(TableName.valueOf(namespace + ":" + table));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean writeToTable(String namespace, TableName tableName, byte[] columnFamily){
        try {
            Table table = connection.getTable(TableName.valueOf(namespace + ":" + tableName));
            int TBL_ROW_COUNT = 100;
            List<Put> puts = new ArrayList<>(TBL_ROW_COUNT);

            for (int row = 0; row != TBL_ROW_COUNT; ++row) {
                byte[] bs = Bytes.toBytes(row);
                Put put = new Put(bs);
                put.addColumn(columnFamily, bs, bs);
                puts.add(put);
            }
            table.put(puts);

            return readTable(namespace, tableName, columnFamily, TBL_ROW_COUNT);

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Boolean readTable(String namespace, TableName tableName, byte[] columnFamily, int TBL_ROW_COUNT) {
        try {
            Table table = connection.getTable(TableName.valueOf(namespace + ":" + tableName));
            Scan scan = new Scan();
            scan.addFamily(columnFamily);

            ResultScanner results = table.getScanner(scan);

            int rowCount = 0;
            if(results != null){
                for (Result result = results.next(); result != null; result = results.next())
                    rowCount++;
            }

            return rowCount == TBL_ROW_COUNT;

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    public Boolean deleteTable(TableDescriptor td) {
        try {
            admin.disableTable(td.getTableName());
            admin.deleteTable(td.getTableName());

            return admin.tableExists(td.getTableName());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Boolean isReachable(Map<String, String> properties) {
        return !connection.isClosed();
    }
}
