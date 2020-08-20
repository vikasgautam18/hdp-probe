package com.gautam.mantra;

import com.gautam.mantra.hdfs.ProbeHDFS;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProbeMain {
    public static Yaml yaml = new Yaml();

    public static void main(String[] args) {

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = loader.getResourceAsStream("cluster-conf.yml");

        Map<String, String> obj = yaml.load(inputStream);

        if(!obj.getOrDefault("debugFlag", "False").equals("True"))
            Logger.getLogger("org").setLevel(Level.OFF);


        ProbeHDFS hdfs = new ProbeHDFS();
        Boolean isReachable = hdfs.isReachable(obj);
        Logger.getLogger(com.gautam.mantra.ProbeMain.class.getName()).info(getTime() + "HDFS is reachable !");
        System.out.println("HDFS is reachable: " + isReachable);
    }

    private static LocalDateTime getTime(){
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        return LocalDateTime.now();
    }
}
