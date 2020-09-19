package com.gautam.mantra.kafka;

import com.gautam.mantra.commons.Event;
import com.gautam.mantra.commons.ProbeService;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.sql.Timestamp;
import java.time.Duration;
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

    /**
     * This method publishes data to a given kafka topic and verifies if the data was indeed written
     * @param topicName the topic to be writtent o
     * @param dataset the data to be published
     * @return True if the data is published successfully, false otherwise
     */
    public boolean publishToTopic(String topicName, List<String> dataset){
        boolean publishResult;
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, properties.get(ProducerConfig.ACKS_CONFIG));
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                properties.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));

        // Producer Instance
        KafkaProducer<Long, String> producer = new KafkaProducer<>(props);

        dataset.forEach(data -> {
            ProducerRecord<Long, String> record = new ProducerRecord<>(topicName, new Date().getTime(), data);
            producer.send(record, (recordMetadata, e) -> {
                if(e != null){
                    logger.error("Error producing record:: ", e);
                } else {
                    logger.debug(String.format("Message was successfully produced to offset %s on partition %s at timestamp %s",
                            recordMetadata.offset(), recordMetadata.partition(), recordMetadata.timestamp()));
                }
            });
        });

        producer.close();

        return readFromTopic(topicName);
    }


    /**
     * This method reads data from a topic and timesout if no data arrived after 3 consecutive polls of 10 seconds each
     * @param topicName the topic to read from
     *
     * @return True if the data was read successfully, false otherwise
     */
    public boolean readFromTopic(String topicName){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                String.join("group-", String.valueOf(System.currentTimeMillis())));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(topicName));

        int numPolls = 3;
        int recordCount = 0;
        while(true){
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(10));
            if(records.isEmpty() && numPolls > 0){
                logger.info(String.format("numPolls:: %d", numPolls));
                numPolls--;
            } else if (numPolls == 0){
                logger.info(String.format("numPolls:: %d", numPolls));
                break;
            }
            else {
                recordCount = recordCount + records.count();
                logger.info(String.format("recordCount :: %d", recordCount));
            }
        }

        consumer.close();

        return (recordCount == Integer.parseInt(properties.get("kafka.probe.records")));
    }

    /**
     * a wrapper method to get admin client
     * @return the AdminClient object
     */
    public AdminClient getAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
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
