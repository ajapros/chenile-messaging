package org.chenile.pubsub.azure.sub;


import com.azure.messaging.eventhubs.models.EventContext;
import org.chenile.core.event.EventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.chenile.pubsub.azure.constants.ChenileKafkaConstants.CHENILE_TOPIC_KEY;


public class AzureEventHubSubscriber implements Consumer<EventContext>, InitializingBean {


    private static final Logger LOGGER = LoggerFactory.getLogger(AzureEventHubSubscriber.class);

    private final EventProcessor eventProcessor;

    public AzureEventHubSubscriber(EventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
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

        // Convert properties to Map<String,String>
        Map<String, String> propertiesMap = convert(eventContext.getEventData().getProperties());
        String topic = propertiesMap.get(CHENILE_TOPIC_KEY);

        try {
            eventProcessor.handleEvent(topic, body, propertiesMap);

            // Update checkpoint after successful processing
            eventContext.updateCheckpoint();
            LOGGER.info("Checkpoint updated for partition {}", eventContext.getPartitionContext().getPartitionId());
        } catch (Exception e) {
            LOGGER.error("Error processing event: {}", body, e);
            // Decide whether to retry, dead-letter, or propagate
            throw new RuntimeException(e);
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

    @Override
    public void afterPropertiesSet() throws Exception {

    }
}
