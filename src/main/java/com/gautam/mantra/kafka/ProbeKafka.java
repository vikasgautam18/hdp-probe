package com.gautam.mantra.kafka;

import com.gautam.mantra.commons.Event;
import com.gautam.mantra.commons.ProbeService;
import org.apache.kafka.clients.admin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class ProbeKafka implements ProbeService {
    private Map<String, String> properties;
    public static final Logger logger =
            LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

    public ProbeKafka(Map<String, String> properties){
        this.properties = properties;
    }

    /**
     * This method verifies if Kafka cluster is reachable
     * @return True if reachable, false otherwise
     */
    @Override
    public Boolean isReachable() {
        try(final AdminClient adminClient = getAdminClient()){
            logger.info(String.format("Kafka Cluster Id ::%s", adminClient.describeCluster().clusterId().get()));
            return !adminClient.describeCluster().clusterId().get().isEmpty();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * This method creates a topic and verifies if it is actually created
     * @param topicName the topic to be created
     * @return True of the topic is successfully created, false otherwise
     */
    public Boolean createTopic(String topicName) {
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
        try(final AdminClient adminClient = getAdminClient()){
            final CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
            result.values().get(topicName).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return topicExists(topicName);
    }

    /**
     * This method verifies if the topic exists
     * @param topicName the name of topic to be verified
     * @return True if exists, false otherwise
     */
    public boolean topicExists(String topicName) {
        try(final AdminClient adminClient = getAdminClient()){
            return adminClient.listTopics().names().get().contains(topicName);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * this method describes the topic
     * @param topicName name of the topic to be described
     */
    public void describeTopic(String topicName) {
        try(final AdminClient adminClient = getAdminClient()){
            adminClient.describeTopics(Collections.singleton(topicName)).values().forEach((topic, topicDescriptionKafkaFuture) -> {
                try {
                    logger.info(String.format("%s :: %s", topic, topicDescriptionKafkaFuture.get().toString()));
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    /**
     * this method deletes a given topic
     * @param topicName name of the topic to be deleted
     * @return True if the topic was successfully deleted, false otherwise
     */
    public boolean deleteTopic(String topicName){
        try(final AdminClient adminClient = getAdminClient()){
            final DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(topicName));
            result.values().get(topicName).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return !topicExists(topicName);
    }

    // TODO: write records to a topic
    // TODO: read from topic

    /**
     * a wrapper method to get admin client
     * @return the AdminClient object
     */
    public AdminClient getAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("kafka.bootstrap.servers"));
        return AdminClient.create(props);
    }

    /**
     * This method generate a list of Events
     * @return list of events
     */
    public List<String> generateEventDataset(){
        List<String> list = new ArrayList<>();
        int numRecords = Integer.parseInt(properties.getOrDefault("kafka.probe.records", "100"));

        for (int i = 0; i < numRecords; i++) {
            list.add(new Event("event-" + i, new Timestamp(System.currentTimeMillis())).toJSON());
        }
        return list;
    }

    /**
     * a utility method to print properties
     */
    public void printProperties(){
        properties.forEach((k, v) -> System.out.println(k + " -> " + v));
    }
}
