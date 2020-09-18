package com.gautam.mantra.kafka;

import com.gautam.mantra.commons.Event;
import com.gautam.mantra.commons.ProbeService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class ProbeKafka implements ProbeService {
    private Map<String, String> properties;

    public ProbeKafka(Map<String, String> properties){
        this.properties = properties;
    }

    @Override
    public Boolean isReachable() {
        try(final AdminClient adminClient = getAdminClient()){
            System.out.println(adminClient.describeCluster().clusterId().get());
            return !adminClient.describeCluster().clusterId().get().isEmpty();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Boolean createTopic(String topicName) {
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
        try(final AdminClient adminClient = getAdminClient()){
            adminClient.createTopics(Collections.singleton(newTopic));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return topicExists(topicName);
    }

    public boolean topicExists(String topicName) {
        try(final AdminClient adminClient = getAdminClient()){
            return adminClient.listTopics().names().get().contains(topicName);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean describeTopic(String topicName) {
        try(final AdminClient adminClient = getAdminClient()){
            adminClient.describeTopics(Collections.singleton(topicName)).values().forEach((topic, topicDescriptionKafkaFuture) -> {
                try {
                    System.out.printf("%s :: %s%n", topic, topicDescriptionKafkaFuture.get().toString());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
        }
        return false;
    }

    public boolean deleteTopic(String topicName){
        try(final AdminClient adminClient = getAdminClient()){
            adminClient.deleteTopics(Collections.singleton(topicName));
        }
        return topicExists(topicName);
    }

    // write records to a topic

    // describe topic

    // read from topic

    public AdminClient getAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("kafka.bootstrap.servers"));
        return AdminClient.create(props);
    }

    public List<String> generateEventDataset(){
        List<String> list = new ArrayList<>();
        int numRecords = Integer.parseInt(properties.getOrDefault("kafka.probe.records", "100"));

        for (int i = 0; i < numRecords; i++) {
            list.add(new Event("event-" + i, new Timestamp(System.currentTimeMillis())).toJSON());
        }
        return list;
    }

    public void printProperties(){
        properties.forEach((k, v) -> System.out.println(k + " -> " + v));
    }
}
