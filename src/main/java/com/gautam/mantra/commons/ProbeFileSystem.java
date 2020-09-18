package com.gautam.mantra.commons;


public interface ProbeFileSystem {

    Boolean createFolder();
    Boolean createFile();
    Boolean readFile();
    Boolean deleteFile();
    Boolean deleteFolder();
    Boolean updatePermissions();
    void cleanup();

}
