package com.gautam.mantra.spark;

import com.gautam.mantra.commons.Utilities;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

class ProbeSparkTest {

    static Map<String, String> properties;
    public static final Yaml yaml = new Yaml();
    static final Utilities utilities = new Utilities();

    @BeforeEach
    void setUp() {
        InputStream inputStream = ProbeSparkTest.class.getClassLoader().getResourceAsStream("cluster-conf.yml");
        properties = yaml.load(inputStream);
        utilities.printProperties(properties);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void submitJob() {
        ProbeSpark spark = new ProbeSpark();
        spark.submitJob(properties);
    }
}