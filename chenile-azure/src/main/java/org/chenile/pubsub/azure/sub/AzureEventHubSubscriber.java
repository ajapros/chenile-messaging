package org.chenile.pubsub.azure.sub;


import com.azure.messaging.eventhubs.models.EventContext;
import org.chenile.core.context.ChenileExchange;
import org.chenile.core.context.ContextContainer;
import org.chenile.core.context.HeaderUtils;
import org.chenile.core.event.EventProcessor;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.azure.configuration.ChenileEventHubProperties;
import org.chenile.pubsub.interceptor.PubSubDirection;
import org.chenile.pubsub.interceptor.PubSubMessage;
import org.chenile.pubsub.interceptor.PubSubMessageInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.chenile.pubsub.azure.constants.ChenileKafkaConstants.CHENILE_TOPIC_KEY;


public class AzureEventHubSubscriber implements Consumer<EventContext>, InitializingBean {


    private static final Logger LOGGER = LoggerFactory.getLogger(AzureEventHubSubscriber.class);

    private final EventProcessor eventProcessor;

    private final ChenilePub chenilePub;

    private final ChenileEventHubProperties chenileEventHubProperties;
    private final List<PubSubMessageInterceptor> interceptors;

    public AzureEventHubSubscriber(EventProcessor eventProcessor, ChenilePub chenilePub,
                                   ChenileEventHubProperties chenileEventHubProperties) {
        this(eventProcessor, chenilePub, chenileEventHubProperties, Collections.emptyList());
    }

    public AzureEventHubSubscriber(EventProcessor eventProcessor, ChenilePub chenilePub,
                                   ChenileEventHubProperties chenileEventHubProperties,
                                   List<PubSubMessageInterceptor> interceptors) {
        this.eventProcessor = eventProcessor;
        this.chenilePub = chenilePub;
        this.chenileEventHubProperties = chenileEventHubProperties;
        this.interceptors = interceptors == null ? Collections.emptyList() : interceptors;
    }

    @Override
    public void accept(EventContext eventContext) {
        String body = eventContext.getEventData().getBodyAsString();
        LOGGER.info("Received event: {}", body);

        // Access custom properties
        eventContext.getEventData().getProperties()
                .forEach((key, val) ->
                        LOGGER.info("Property: {} = {}", key, val)
                );

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("azure.partitionId", eventContext.getPartitionContext().getPartitionId());
        metadata.put("azure.sequenceNumber", eventContext.getEventData().getSequenceNumber());

        PubSubMessage message = applyBeforeSubscribeInterceptors(
                new PubSubMessage(
                        null,
                        body,
                        eventContext.getEventData().getProperties(),
                        PubSubDirection.SUBSCRIBE,
                        "azure",
                        metadata
                )
        );

        // Convert properties to Map<String,String>
        Map<String, String> propertiesMap = convert(message.getHeaders());
        String topic = propertiesMap.get(CHENILE_TOPIC_KEY);

        try {
            List<ChenileExchange> resList = processWithTenantContext(topic, message.getPayload(), propertiesMap);
            processDeadLetters(body, eventContext.getEventData().getProperties(), resList);
            LOGGER.info("Checkpoint updated for partition {}", eventContext.getPartitionContext().getPartitionId());
            eventContext.updateCheckpoint();
        } catch (Exception e) {
            LOGGER.error("Error processing event: {}", body, e);
            throw new RuntimeException(e);
        }
    }

    void processDeadLetters(String body, Map<String, Object> eventProperties, List<ChenileExchange> results) {
        for (ChenileExchange res : results) {
            if (res.getException() != null) {
                Map<String, Object> props = new HashMap<>(eventProperties);
                props.put("e", res.getException().getMessage());
                chenilePub.asyncPublish(chenileEventHubProperties.getDl(), body, props);
            }
        }
    }

    // Helper method to convert Map<String,Object> → Map<String,String>
    private static Map<String, String> convert(Map<String, Object> input) {
        return input.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> String.valueOf(e.getValue()) // handles null safely
                ));
    }

    List<ChenileExchange> processWithTenantContext(String topic, String body, Map<String, String> propertiesMap) {
        ContextContainer contextContainer = ContextContainer.getInstance();
        String previousTenant = contextContainer.getTenant();
        String tenant = propertiesMap.get(HeaderUtils.TENANT_ID_KEY);
        if (tenant != null && tenant.isBlank()) {
            tenant = null;
        }
        try {
            contextContainer.setTenant(tenant);
            return eventProcessor.handleEvent(topic, body, propertiesMap);
        } finally {
            contextContainer.setTenant(previousTenant);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }

    private PubSubMessage applyBeforeSubscribeInterceptors(PubSubMessage message) {
        PubSubMessage current = message;
        for (PubSubMessageInterceptor interceptor : interceptors) {
            PubSubMessage intercepted = interceptor.beforeSubscribe(current);
            if (intercepted == null) {
                throw new IllegalStateException("PubSubMessageInterceptor returned null from beforeSubscribe");
            }
            current = intercepted;
        }
        return current;
    }
}
