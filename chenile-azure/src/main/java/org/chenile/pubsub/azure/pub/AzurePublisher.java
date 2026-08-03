package org.chenile.pubsub.azure.pub;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import org.chenile.base.exception.ServerException;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.azure.configuration.ChenileEventHubProperties;
import org.chenile.pubsub.azure.util.EventHubNameUtils;
import org.chenile.pubsub.errorcodes.ErrorCodes;
import org.chenile.pubsub.interceptor.PubSubDirection;
import org.chenile.pubsub.interceptor.PubSubMessage;
import org.chenile.pubsub.interceptor.PubSubMessageInterceptor;
import org.chenile.pubsub.model.ChenilePubSub;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.*;

import static org.chenile.pubsub.azure.constants.ChenileAzureConstants.*;

/**
 * Azure-based implementation of the {@link ChenilePub} interface.
 * Provides methods to publish messages to Azure hub topics synchronously or asynchronously.
 */
public class AzurePublisher implements ChenilePub {

    private final PubSubInfoProvider pubSubInfoProvider;
    private final ChenileEventHubProperties chenileEventHubProperties;
    private final List<PubSubMessageInterceptor> interceptors;

    @Autowired
    private Map<String, EventHubProducerClient> producerClients;

    public AzurePublisher(PubSubInfoProvider pubSubInfoProvider,
                          ChenileEventHubProperties chenileEventHubProperties) {
        this(pubSubInfoProvider, chenileEventHubProperties, Collections.emptyList());
    }

    public AzurePublisher(PubSubInfoProvider pubSubInfoProvider,
                          ChenileEventHubProperties chenileEventHubProperties,
                          List<PubSubMessageInterceptor> interceptors) {
        this.pubSubInfoProvider = pubSubInfoProvider;
        this.chenileEventHubProperties = chenileEventHubProperties;
        this.interceptors = interceptors == null ? Collections.emptyList() : interceptors;
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
        String physicalTopic = chenileEventHubProperties.resolvePhysicalHubName(topic);
        String resolvedTopic = physicalTopic;
        if (chenileEventHubProperties.isClientPrefixEnabled()) {
            resolvedTopic = EventHubNameUtils.resolveHubName(
                    physicalTopic,
                    properties,
                    chenileEventHubProperties.getClients(),
                    chenileEventHubProperties.getClientPrefixSeparator()
            );
        }
        // Check if the producer client for the topic exists
        if (!producerClients.containsKey(resolvedTopic) || producerClients.get(resolvedTopic) == null) {
            throw new IllegalStateException(
                    "Azure Event Hub client for topic '" + resolvedTopic + "' is not registered. " +
                            "Please add it to the configuration and ensure it is available in the cloud."
            );
        }

        PubSubMessage message = applyBeforePublishInterceptors(
                new PubSubMessage(
                        topic,
                        payload,
                        buildHeaders(topic, properties),
                        PubSubDirection.PUBLISH,
                        "azure",
                        Map.of("azure.physicalHub", resolvedTopic)
                )
        );

        EventData eventData = new EventData(message.getPayload());
        eventData.getProperties().putAll(message.getHeaders());

        CreateBatchOptions createBatchOptions = new CreateBatchOptions();
        applyPartitionRouting(createBatchOptions, properties);

        EventDataBatch batch = producerClients.get(resolvedTopic).createBatch(createBatchOptions);

        if (!batch.tryAdd(eventData)) {
            throw new IllegalStateException("Event is too large for batch");
        }

        producerClients.get(resolvedTopic).send(batch);
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
        Map<String, Object> headers = new HashMap<>();
        if (properties != null && !properties.isEmpty()) {
            headers.putAll(properties);
        }
        headers.put(CHENILE_TOPIC_KEY, topic);
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

    /** Configures default, explicit-ID, Azure-keyed, or automatic Event Hubs routing. */
    private static void applyPartitionRouting(CreateBatchOptions options, Map<String, Object> properties) {
        String partitionId = getPartitionId(properties);
        String partitionKey = getPartitionKey(properties);
        String partitionMode = getPartitionMode(properties);
        if (partitionId != null && partitionKey != null) {
            throw new IllegalArgumentException("Only one partition selector may be supplied: "
                    + CHENILE_AZURE_PARTITION_ID + " or " + CHENILE_AZURE_PARTITION_KEY);
        }
        if (partitionMode != null && !CHENILE_AZURE_PARTITION_MODE_AUTO.equals(partitionMode)) {
            throw new IllegalArgumentException("Unsupported Azure partition mode '" + partitionMode
                    + "'. Supported mode: " + CHENILE_AZURE_PARTITION_MODE_AUTO);
        }
        if (CHENILE_AZURE_PARTITION_MODE_AUTO.equals(partitionMode)) {
            if (partitionId != null || partitionKey != null) {
                throw new IllegalArgumentException(CHENILE_AZURE_PARTITION_MODE_AUTO
                        + " partition mode cannot be combined with an explicit partition selector");
            }
            return;
        }
        if (partitionId != null) {
            options.setPartitionId(partitionId);
        } else if (partitionKey != null) {
            options.setPartitionKey(partitionKey);
        } else {
            options.setPartitionId("0");
        }
    }

    /**
     * Extracts an explicit partition number. An absent value is handled by the default partition
     * zero route; an invalid supplied value retains the historic fallback to partition zero.
     */
    private static String getPartitionId(Map<String, Object> properties) {
        if (properties == null) return null;
        Object partitionObj = properties.get(CHENILE_AZURE_PARTITION_ID);
        if (partitionObj == null || partitionObj.toString().isBlank()) return null;
        try {
            return String.valueOf(Integer.parseInt(partitionObj.toString()));
        } catch (NumberFormatException e) {
            return String.valueOf(0); // fallback to default
        }
    }

    private static String getPartitionKey(Map<String, Object> properties) {
        if (properties == null) return null;
        Object partitionKey = properties.get(CHENILE_AZURE_PARTITION_KEY);
        if (partitionKey == null || partitionKey.toString().isBlank()) return null;
        return partitionKey.toString();
    }

    private static String getPartitionMode(Map<String, Object> properties) {
        if (properties == null) return null;
        Object partitionMode = properties.get(CHENILE_AZURE_PARTITION_MODE);
        if (partitionMode == null || partitionMode.toString().isBlank()) return null;
        return partitionMode.toString();
    }

    private PubSubMessage applyBeforePublishInterceptors(PubSubMessage message) {
        PubSubMessage current = message;
        for (PubSubMessageInterceptor interceptor : interceptors) {
            PubSubMessage intercepted = interceptor.beforePublish(current);
            if (intercepted == null) {
                throw new IllegalStateException("PubSubMessageInterceptor returned null from beforePublish");
            }
            current = intercepted;
        }
        return current;
    }
}
