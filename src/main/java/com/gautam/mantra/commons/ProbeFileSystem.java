package com.gautam.mantra.commons;

import java.util.Map;

public interface ProbeFileSystem {

    Boolean createFolder(Map<String, String> props);
    Boolean createFile(Map<String, String> props);
    Boolean readFile(Map<String, String> props);
    Boolean deleteFile(Map<String, String> props);
    Boolean deleteFolder(Map<String, String> props);
    Boolean updatePermissions(Map<String, String> props);
    void cleanup(Map<String, String> props);

}
