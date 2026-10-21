package org.chenile.pubsub.kafka.impl;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.chenile.base.exception.ServerException;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.errorcodes.ErrorCodes;
import org.chenile.pubsub.model.ChenilePubSub;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.chenile.pubsub.kafka.constants.ChenileKafkaConstants.*;

/**
 * Kafka-based implementation of the {@link ChenilePub} interface.
 * Provides methods to publish messages to Kafka topics synchronously or asynchronously.
 */
public class KafkaPublisher implements ChenilePub {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final PubSubInfoProvider pubSubInfoProvider;
    private final long waitTimeMillis;

    @Autowired
    public KafkaPublisher(KafkaTemplate<String, String> kafkaTemplate,
                          PubSubInfoProvider pubSubInfoProvider) {
        this(kafkaTemplate, pubSubInfoProvider, 0);
    }

    public KafkaPublisher(KafkaTemplate<String, String> kafkaTemplate,
                          PubSubInfoProvider pubSubInfoProvider,
                          long waitTimeMillis) {
        this.kafkaTemplate = kafkaTemplate;
        this.pubSubInfoProvider = pubSubInfoProvider;
        this.waitTimeMillis = waitTimeMillis;
        if (this.waitTimeMillis < 0) {
            throw new IllegalArgumentException("pubsub.publish.waitTime must not be negative");
        }
    }
    /**
     * Publishes a message to the given service's operation topic.
     *
     * @param service       the service name
     * @param operationName the operation name
     * @param payload       the message payload
     * @param properties    additional message properties
     */
    @Override
    public void publishToOperation(String service, String operationName, String payload,
                                   Map<String, Object> properties) {

        ChenilePubSub pubSubInfo = pubSubInfoProvider.obtainChenileMqtt(service);
        if (pubSubInfo == null) {
            throw new ServerException(ErrorCodes.CANNOT_FIND_TOPIC.getSubError(),
                    new Object[]{service});
        }

        String topic = substituteProperties(pubSubInfo.publishTopic(), properties);
        topic = topic + "_" + operationName;

        publish(topic, payload, properties);
    }

    /**
     * Publishes a message asynchronously to the given Kafka topic.
     *
     * @param topic      the Kafka topic
     * @param payload    the message payload
     * @param properties additional message properties
     */
    @Override
    public void asyncPublish(String topic, String payload, Map<String, Object> properties) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                getPartition(properties),
                "message",
                payload,
                buildHeaders(topic, properties)
        );

        send(record);
    }

    /**
     * Publishes a message to the global Kafka topic.
     *
     * @param topic      logical topic name (added as header, not used as Kafka topic)
     * @param payload    the message payload
     * @param properties additional message properties
     */
    @Override
    public void publish(String topic, String payload, Map<String, Object> properties) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                CHENILE_GLOBAL_TOPIC,
                getPartition(properties),
                "message",
                payload,
                buildHeaders(topic, properties)
        );

        send(record);
    }

    private void send(ProducerRecord<String, String> record) {
        var result = kafkaTemplate.send(record);
        if (waitTimeMillis == 0) {
            return;
        }
        try {
            result.get(waitTimeMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka publication interrupted", e);
        } catch (Exception e) {
            throw new IllegalStateException("Kafka publication failed", e);
        }
    }

    /**
     * Builds Kafka headers from message properties and adds the Chenile topic key.
     *
     * @param topic      the logical topic
     * @param properties message properties
     * @return list of Kafka headers
     */
    private static List<Header> buildHeaders(String topic, Map<String, Object> properties) {
        List<Header> headers = new ArrayList<>();
        if (properties != null) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                if (entry.getValue() != null) {
                    headers.add(new RecordHeader(
                            entry.getKey(),
                            String.valueOf(entry.getValue()).getBytes(StandardCharsets.UTF_8))
                    );
                }
            }
        }
        headers.add(new RecordHeader(
                CHENILE_TOPIC_KEY,
                topic.getBytes(StandardCharsets.UTF_8))
        );
        return headers;
    }

    /**
     * Replaces placeholders in a string with values from the given properties map.
     *
     * @param text       the string containing placeholders like {key}
     * @param properties the map of properties
     * @return the resolved string
     */
    private static String substituteProperties(String text, Map<String, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return text;
        }
        String result = text;
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            result = result.replace("{" + entry.getKey() + "}", String.valueOf(entry.getValue()));
        }
        return result;
    }

    /**
     * Extracts the partition number from the properties map.
     *
     * @param properties the message properties
     * @return the partition number (default = 0)
     */
    private static Integer getPartition(Map<String, Object> properties) {
        if (properties == null) {
            return 0;
        }
        Object partitionObj = properties.get(CHENILE_KAFKA_PARTITION_KEY);
        if (partitionObj == null) {
            return 0;
        }
        try {
            return Integer.parseInt(partitionObj.toString());
        } catch (NumberFormatException e) {
            return 0; // fallback to default
        }
    }
}
