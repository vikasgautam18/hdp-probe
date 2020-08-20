package com.gautam.mantra.commons;

public interface ProbeFileSystem {

    Boolean createFolder(String folderName);
    Boolean createFile(String fileName);
    Boolean readFile(String fileName);
    Boolean deleteFile(String fileName);
    Boolean updatePermissions(String fileName);

}
