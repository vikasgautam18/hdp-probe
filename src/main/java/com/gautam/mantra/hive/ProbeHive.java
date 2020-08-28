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

    /**
     * Creates and verifies a hive database
     * @param database the name of database
     * @return true if the database was created successfully, false otherwise
     */
    public boolean createDatabase(String database){
        try {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + database);
            return databaseExists(database);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return false;
        }
    }

    /**
     * This method finds out if the given database exists
     * @param database the name of database
     * @return true if the database exists, false otherwise
     */
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

    /**
     * Creates and verifies a table in hive
     * @param sql the sql statement to create table
     * @param database the name of database
     * @param table the name of table
     * @return true if the table was created, false otherwise
     */
    public boolean createTable(String sql, String database, String table){
        try {
            statement.execute(sql);
            return tableExists(database, table);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return false;
        }
    }

    /**
     * Verifies if a hive table exists
     * @param database the name of database
     * @param table the name of table
     * @return true if the table exists, false otherwise
     */
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

    /**
     * Load data to Hive table
     * @param properties the cluster configuration
     */
    public void writeToTable(Map<String, String> properties){
        try {
            String filepath = properties.get("hiveTestFilePath");
            String sql = "load data local inpath '" + filepath + "' into table db_test.table_test" ;
            statement.execute(sql);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    /**
     * Drops a table and verifies if the table was indeed dropped
     * @param database the name of database
     * @param table the name of table to be dropped
     * @return true if the table was dropped successfully, false otherwise
     */
    public boolean dropTable(String database, String table){
        try {
            statement.execute("drop table if exists " + String.join(".", database, table));
            return !tableExists(database, table);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return false;
        }
    }

    /**
     * This method drops a database from hive
     * @param database the name of database to be dropped
     * @param cascade True if child tables need to be dropped as well
     * @return true of the database was dropped successfully, false otherwise
     */
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
