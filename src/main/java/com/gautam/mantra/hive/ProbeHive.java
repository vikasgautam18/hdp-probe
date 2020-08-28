package com.gautam.mantra.hive;

import java.sql.*;

public class ProbeHive {

    private static Statement statement;

    public ProbeHive(){
        try {
            String driverName = "org.apache.hive.jdbc.HiveDriver";
            Class.forName(driverName);
            Connection jdbcConnection = DriverManager.getConnection("jdbc:hive2:///", "", "");
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

    public boolean createTable(String database, String table){
        try {
            String tableNameWithDB = String.join(".", database, table);
            statement.execute("create table " + tableNameWithDB + " (key int, value string)");

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


}
