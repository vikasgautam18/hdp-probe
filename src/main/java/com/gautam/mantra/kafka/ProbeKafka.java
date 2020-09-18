package com.gautam.mantra.kafka;

import com.gautam.mantra.commons.ProbeService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProbeKafka implements ProbeService {
    private Map<String, String> properties;

    public ProbeKafka(Map<String, String> properties){
        this.properties = properties;
    }


    @Override
    public Boolean isReachable() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("kafka.bootstrap.servers"));

        try(final AdminClient adminClient = AdminClient.create(props)){
            System.out.println(adminClient.describeCluster().clusterId().get());
            return !adminClient.describeCluster().clusterId().get().isEmpty();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void printProperties(){
        properties.forEach((k, v) -> System.out.println(k + " -> " + v));
    }
}
