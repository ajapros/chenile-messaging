package org.chenile.pubsub.azure.pub;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import org.chenile.base.exception.ServerException;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.errorcodes.ErrorCodes;
import org.chenile.pubsub.model.ChenilePubSub;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.*;

import static org.chenile.pubsub.azure.constants.ChenileKafkaConstants.*;

/**
 * Azure-based implementation of the {@link ChenilePub} interface.
 * Provides methods to publish messages to Azure hub topics synchronously or asynchronously.
 */
public class AzurePublisher implements ChenilePub {

    private final PubSubInfoProvider pubSubInfoProvider;

    @Autowired
    private Map<String, EventHubProducerClient> producerClients;

    public AzurePublisher(PubSubInfoProvider pubSubInfoProvider) {
        this.pubSubInfoProvider = pubSubInfoProvider;
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

        sendMessage(topic, payload, properties);
    }

    private void sendMessage(String topic, String payload, Map<String, Object> properties) {
        EventData eventData = new EventData(payload);
        eventData.getProperties().putAll(buildHeaders(topic, properties));

        CreateBatchOptions createBatchOptions = new CreateBatchOptions();
        createBatchOptions.setPartitionId(getPartition(properties));

        EventDataBatch batch = producerClients.get(topic).createBatch(createBatchOptions);

        if (!batch.tryAdd(eventData)) {
            throw new IllegalStateException("Event is too large for batch");
        }

        producerClients.get(topic).send(batch);

        //eventHubProducerClient.send(batch);
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
        sendMessage(topic, payload, properties);

    }

    /**
     * Builds Kafka headers from message properties and adds the Chenile topic key.
     *
     * @param topic      the logical topic
     * @param properties message properties
     * @return list of Kafka headers
     */
    private static Map<String, Object> buildHeaders(String topic, Map<String, Object> properties) {

        properties.put(CHENILE_TOPIC_KEY,topic);

        return properties;
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
    private static String getPartition(Map<String, Object> properties) {
        if (properties == null) {
            return String.valueOf(0);
        }
        Object partitionObj = properties.get(CHENILE_KAFKA_PARTITION_KEY);
        if (partitionObj == null) {
            return String.valueOf(0);
        }
        try {
            return String.valueOf(Integer.parseInt(partitionObj.toString()));
        } catch (NumberFormatException e) {
            return String.valueOf(0); // fallback to default
        }
    }
}
