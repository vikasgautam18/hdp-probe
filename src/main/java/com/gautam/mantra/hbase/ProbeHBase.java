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

    private static Logger logger = LoggerFactory.getLogger(ProbeHBase.class.getName());

    public Connection getHBaseConnection(Configuration hbaseConfiguration){
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(hbaseConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * This method creates an HBase namespace
     * @param namespace the namespace
     * @return true if the namespace was created successfully, false otherwise
     */
    public Boolean createNameSpace(Connection connection, String namespace) {
        try {
            connection.getAdmin().createNamespace(NamespaceDescriptor.create(namespace).build());
            return existsNameSpace(connection, namespace);

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean existsNameSpace(Connection connection, String namespace){
        try {
            NamespaceDescriptor ns = NamespaceDescriptor.create(namespace).build();
            NamespaceDescriptor[] list = connection.getAdmin().listNamespaceDescriptors();
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

    public Boolean deleteNameSpace(Connection connection, String namespace) {
        try {
            if(connection.getAdmin().listTableDescriptorsByNamespace(namespace.getBytes()).size() > 0){
                for (TableDescriptor td: connection.getAdmin().listTableDescriptorsByNamespace(namespace.getBytes())
                     ) {
                    deleteTable(connection, td);
                }
            }

            connection.getAdmin().deleteNamespace(namespace);
            return !existsNameSpace(connection, namespace);

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    public boolean createTable(Connection connection, String namespace, String table, String cf) {
        try {
            connection.getAdmin().createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace + ":" + table))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf))
                    .build());

            return connection.getAdmin().tableExists(TableName.valueOf(namespace + ":" + table));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean writeToTable(Connection connection,String namespace, TableName tableName, byte[] columnFamily){
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
            table.close();
            return readTable(connection, namespace, tableName, columnFamily, TBL_ROW_COUNT);

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Boolean readTable(Connection connection, String namespace, TableName tableName,
                             byte[] columnFamily, int TBL_ROW_COUNT) {
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
            table.close();
            return rowCount == TBL_ROW_COUNT;

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    public Boolean deleteTable(Connection connection, TableDescriptor td) {
        try {
            connection.getAdmin().disableTable(td.getTableName());
            connection.getAdmin().deleteTable(td.getTableName());

            return connection.getAdmin().tableExists(td.getTableName());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Boolean isReachable(Connection connection, Map<String, String> properties) {
        return !connection.isClosed();
    }

    public void closeConnection(Connection connection){
        try {
            connection.getAdmin().close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
