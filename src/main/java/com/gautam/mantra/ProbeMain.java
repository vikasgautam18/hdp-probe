package com.gautam.mantra;

import com.gautam.mantra.hdfs.ProbeHDFS;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

public class ProbeMain {
    public static Yaml yaml = new Yaml();

    public static void main(String[] args) {

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = loader.getResourceAsStream("cluster-conf.yml");

        Map<String, Object> obj = yaml.load(inputStream);


        ProbeHDFS hdfs = new ProbeHDFS();
        Boolean isReachable = hdfs.isReachable(obj);

        System.out.println("HDFS is reachable: " + isReachable);
    }
}
