package org.chenile.pubsub.azure.sub;


import com.azure.messaging.eventhubs.models.EventContext;
import org.chenile.core.context.ChenileExchange;
import org.chenile.core.context.ContextContainer;
import org.chenile.core.event.EventProcessor;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.azure.configuration.ChenileEventHubProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.io.PrintWriter;
import java.io.StringWriter;
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

    public AzureEventHubSubscriber(EventProcessor eventProcessor, ChenilePub chenilePub,
                                   ChenileEventHubProperties chenileEventHubProperties) {
        this.eventProcessor = eventProcessor;
        this.chenilePub = chenilePub;
        this.chenileEventHubProperties = chenileEventHubProperties;
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
            List<ChenileExchange> resList = eventProcessor.handleEvent(topic, body, propertiesMap);

            for(ChenileExchange res:resList){
                if(res.getException()!=null){
                    Map<String, Object> props =
                            new HashMap<>(eventContext.getEventData().getProperties());

                    props.put("e",res.getException().getMessage());
                    chenilePub.asyncPublish(chenileEventHubProperties.getDl(),body,props);
                }
            }
            // Update checkpoint after successful processing
            LOGGER.info("Checkpoint updated for partition {}", eventContext.getPartitionContext().getPartitionId());
        } catch (Exception e) {
            LOGGER.error("Error processing event: {}", body, e);
            // Decide whether to retry, dead-letter, or propagate
            throw new RuntimeException(e);
        }finally {
            eventContext.updateCheckpoint();
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
