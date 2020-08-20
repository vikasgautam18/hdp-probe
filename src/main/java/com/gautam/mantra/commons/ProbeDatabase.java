package com.gautam.mantra.commons;

public interface ProbeDatabase extends ProbeService{

    public Boolean createDatabase(String database);
    public Boolean deleteDatabase(String database);
    public Boolean createTable(String database, String table);
    public Boolean readTable(String database, String table);
    public Boolean deleteTable(String database, String table);
}
