package com.gautam.mantra.commons;

public interface ProbeFileSystem {

    public Boolean createFolder(String folderName);
    public Boolean createFile(String fileName);
    public Boolean readFile(String fileName);
    public Boolean deleteFile(String fileName);
    public Boolean updatePermissions(String fileName);

}
