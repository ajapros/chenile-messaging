package org.chenile.pubsub.kafka.configuration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.chenile.core.context.ContextContainer;
import org.chenile.core.event.EventProcessor;
import org.chenile.core.model.ChenileConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.kafka.config.AbstractKafkaListenerEndpoint;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.adapter.HandlerAdapter;
import org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Dynamically registers Kafka listeners for configured topic-to-event bindings.
 */
@Component
public class CustomKafkaConsumer implements ApplicationListener<ApplicationReadyEvent>, Ordered {

    private static final Logger logger = LoggerFactory.getLogger(CustomKafkaConsumer.class);

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, String> factory;

    @Autowired
    @Qualifier("chenileServiceConfiguration")
    private ChenileConfiguration chenileConfig;

    @Autowired
    private EventProcessor eventProcessor;

    @Autowired
    private KafkaConsumerProperties consumerProperties;

    /**
     * Registers a dynamic Kafka listener after application startup.
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {

        Map<String, String> topicToEvent = topicToEventBindings();
        if (topicToEvent.isEmpty()) {
            logger.warn("No topics found in Chenile configuration to register Kafka listeners.");
            return;
        }

        logger.info("Registering Kafka listener topic to event bindings: {}", topicToEvent);

        AbstractKafkaListenerEndpoint<String, String> endpoint = new AbstractKafkaListenerEndpoint<>() {
            @Override
            protected MessagingMessageListenerAdapter<String, String> createMessageListener(
                    MessageListenerContainer container,
                    MessageConverter messageConverter) {

                logger.info("Creating Kafka message listener for Chenile events");

                try {
                    KafkaMessageProcessor processor = new KafkaMessageProcessor(eventProcessor, topicToEvent);

                    Method method = KafkaMessageProcessor.class.getMethod("handleMessage", ConsumerRecord.class);
                    MessagingMessageListenerAdapter<String, String> adapter =
                            new RecordMessagingMessageListenerAdapter<>(processor, method, null);

                    InvocableHandlerMethod handlerMethod = new InvocableHandlerMethod(processor, method);
                    HandlerAdapter handler = new HandlerAdapter(handlerMethod);
                    adapter.setHandlerMethod(handler);

                    return adapter;
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        endpoint.setId("chenile_events");
        endpoint.setGroupId(consumerProperties.getGroupId());
        endpoint.setTopics(topicToEvent.keySet().toArray(String[]::new));

        registry.registerListenerContainer(endpoint, factory, true);
    }

    @Override
    public int getOrder() {
        return 910;
    }

    private Map<String, String> topicToEventBindings() {
        Map<String, String> topicToEvent = new LinkedHashMap<>();
        Map<String, String> configuredBindings = consumerProperties.getTopicBindings();
        if (configuredBindings == null || configuredBindings.isEmpty()) {
            chenileConfig.getEvents().keySet().forEach(eventId -> topicToEvent.put(eventId, eventId));
            return topicToEvent;
        }

        configuredBindings.forEach((topic, eventId) -> {
            if (topic == null || topic.isBlank() || eventId == null || eventId.isBlank()) {
                throw new IllegalStateException("Kafka topics and Chenile event IDs must not be blank");
            }
            if (!chenileConfig.getEvents().containsKey(eventId)) {
                throw new IllegalStateException(
                        "Kafka topic '" + topic + "' is bound to unknown Chenile event '" + eventId + "'");
            }
            topicToEvent.put(topic, eventId);
        });
        return topicToEvent;
    }

    /**
     * Processor class for handling Kafka messages.
     */
    static class KafkaMessageProcessor {
        private final EventProcessor eventProcessor;
        private final Map<String, String> topicToEvent;

        KafkaMessageProcessor(EventProcessor eventProcessor, Map<String, String> topicToEvent) {
            this.eventProcessor = eventProcessor;
            this.topicToEvent = topicToEvent;
        }

        public void handleMessage(ConsumerRecord<String, Object> record) {
            logger.debug("Processing message from topic '{}': {}", record.topic(), record.value());
            String eventId = topicToEvent.get(record.topic());
            if (eventId == null) {
                throw new IllegalStateException(
                        "No Chenile event is bound to Kafka topic '" + record.topic() + "'");
            }
            Map<String, String> headers = headersToMap(record.headers());
            ContextContainer context = ContextContainer.CONTEXT_CONTAINER;
            ContextContainer.ContextSnapshot snapshot = context.snapshot();
            try {
                var exchanges = eventProcessor.handleEvent(eventId, record.value(), headers);
                if (exchanges.isEmpty()) {
                    throw new IllegalStateException("No Chenile subscriber handled event '" + eventId + "'");
                }
                exchanges.stream()
                        .filter(exchange -> exchange.getException() != null)
                        .findFirst()
                        .ifPresent(exchange -> {
                            throw new IllegalStateException(
                                    "Chenile event subscriber failed for event '" + eventId + "'",
                                    exchange.getException());
                        });
            } finally {
                context.restore(snapshot);
            }
        }

        static Map<String, String> headersToMap(Headers headers) {
            Map<String, String> map = new HashMap<>();
            for (Header header : headers) {
                String key = header.key();
                String value = header.value() != null
                        ? new String(header.value(), StandardCharsets.UTF_8)
                        : null;
                map.put(key, value);
            }
            return map;
        }
    }
}
