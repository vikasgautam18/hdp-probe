package com.gautam.mantra.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

public class ProbeHBase {

    private static Connection connection;
    public static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

    public ProbeHBase(Configuration conf) {
        try{
        connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method creates an HBase namespace
     * @param namespace the namespace
     * @return true if the namespace was created successfully, false otherwise
     */
    public Boolean createNameSpace(String namespace) {
        try {
            connection.getAdmin().createNamespace(NamespaceDescriptor.create(namespace).build());
            return existsNameSpace(namespace);

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method finds out if the namespace exists
     * @param namespace the namespace
     * @return true if it does exist, false otherwise
     */
    public boolean existsNameSpace(String namespace){
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

    /**
     * This method deletes a given namespace. Also deletes all tables associated
     * @param namespace the namespace
     * @return true if the delete was successful, false otherwise
     */
    public Boolean deleteNameSpace(String namespace) {
        try {
            if(connection.getAdmin().listTableDescriptorsByNamespace(namespace.getBytes()).size() > 0){
                for (TableDescriptor td: connection.getAdmin().listTableDescriptorsByNamespace(namespace.getBytes())
                     ) {
                    if(deleteTable(td))
                        logger.info("deleted table " + td.getTableName().getNameAsString() + " ...");
                }
            }

            connection.getAdmin().deleteNamespace(namespace);
            return !existsNameSpace(namespace);

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method creates an HBase table
     * @param namespace the namespace to use
     * @param table the name of table
     * @param cf the column family
     * @return true if the table creation was successful, false otherwise
     */
    public boolean createTable(String namespace, String table, String cf) {
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

    /**
     * This method writes some dummy rows to a given hbase table
     * @param namespace the namespace to use
     * @param tableName the table to be used
     * @param columnFamily the column family to use
     * @return true if the write to table was successful, false otherwise
     */
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
            table.close();
            return readTable(namespace, tableName, columnFamily, TBL_ROW_COUNT);

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method reads data from a given hbase table and matches the count against expected count
     * @param namespace the namespace
     * @param tableName the table name
     * @param columnFamily the column family
     * @param TBL_ROW_COUNT the given table row count
     * @return true if the read count was same as expected
     */
    public Boolean readTable(String namespace, TableName tableName,
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


    /**
     * this method deletes a given table
     * @param td table description
     * @return true if the delete was successful, false otherwise
     */
    public Boolean deleteTable(TableDescriptor td) {
        try {
            connection.getAdmin().disableTable(td.getTableName());
            connection.getAdmin().deleteTable(td.getTableName());

            return connection.getAdmin().tableExists(td.getTableName());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Finds out if HBase is reachable
     * @param conf the HBase configuration object
     * @return true if HBase is reachable, false otherwise
     */
    public Boolean isReachable(Configuration conf) {
        try {
            HBaseAdmin.available(conf);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void closeConnection(){
        try {
            connection.getAdmin().close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
