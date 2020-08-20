package com.gautam.mantra.commons;

public interface ProbeDatabase extends ProbeService{

    Boolean createDatabase(String database);
    Boolean deleteDatabase(String database);
    Boolean createTable(String database, String table);
    Boolean readTable(String database, String table);
    Boolean deleteTable(String database, String table);
}
