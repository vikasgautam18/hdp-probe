package com.gautam.mantra.hbase;

import com.gautam.mantra.commons.ProbeDatabase;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
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
        try {
            Admin admin = connection.getAdmin();
            admin.createNamespace(NamespaceDescriptor.create(database).build());
            return existsNameSpace(database);

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean existsNameSpace(String namespace){
        try {
            Admin admin = connection.getAdmin();

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

    @Override
    public Boolean deleteDatabase(String database) {
        try {
            Admin admin = connection.getAdmin();
            admin.deleteNamespace(database);
            return !existsNameSpace(database);

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
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
