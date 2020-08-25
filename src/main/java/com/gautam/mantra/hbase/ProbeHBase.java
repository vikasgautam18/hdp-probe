package com.gautam.mantra.hbase;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Map;

public class ProbeHBase {

    public static Connection connection;
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
                    admin.disableTable(td.getTableName());
                    admin.deleteTable(td.getTableName());
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


    public Boolean readTable(String database, String table) {
        return null;
    }


    public Boolean deleteTable(String namespace, String table) {
        try {
            admin.disableTable(TableName.valueOf(namespace + ":" + table));
            admin.deleteTable(TableName.valueOf(namespace + ":" + table));

            return admin.tableExists(TableName.valueOf(namespace + ":" + table));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Boolean isReachable(Map<String, String> properties) {
        return !connection.isClosed();
    }
}
