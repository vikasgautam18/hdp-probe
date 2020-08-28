package com.gautam.mantra.hive;

import java.sql.*;
import java.util.Map;

public class ProbeHive {

    private static Statement statement;

    public ProbeHive(Map<String, String> properties){
        try {
            String driverName = "org.apache.hive.jdbc.HiveDriver";
            Class.forName(driverName);
            Connection jdbcConnection = DriverManager.getConnection(properties.get("hiveJDBCURL"), "", "");
            statement = jdbcConnection.createStatement();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public boolean createDatabase(String database){
        try {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + database);
            return databaseExists(database);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return false;
        }
    }

    public boolean databaseExists(String database){
        try {
            ResultSet resultSet = statement.executeQuery("show databases");
            boolean returnVal = false;
            while(resultSet.next()){
                if(resultSet.getString("database_name").equals(database)) {
                    returnVal = true;
                    break;
                }
            }
            return returnVal;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return false;
        }
    }

    public boolean createTable(String sql, String database, String table){
        try {
            statement.execute(sql);
            return tableExists(database, table);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return false;
        }
    }

    public boolean tableExists(String database, String table){
        try {
            ResultSet resultSet = statement.executeQuery("show tables in " + database);
            boolean returnVal = false;
            while (resultSet.next()){
                if(resultSet.getString("tab_name").equals(table)) {
                    returnVal = true;
                    break;
                }
            }
            return returnVal;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return false;
        }
    }

    public void writeToTable(Map<String, String> properties){
        try {
            String filepath = properties.get("hiveTestFilePath");
            String sql = "load data local inpath '" + filepath + "' into table db_test.table_test" ;
            statement.execute(sql);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public boolean dropTable(String database, String table){
        try {
            statement.execute("drop table if exists " + String.join(".", database, table));
            return !tableExists(database, table);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return false;
        }
    }

    public boolean dropDatabase(String database, boolean cascade){
        try {
            String CASCADE = cascade ? " CASCADE": "";
            statement.execute("drop database if exists " + database + CASCADE);
            return !databaseExists(database);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return false;
        }
    }

}
