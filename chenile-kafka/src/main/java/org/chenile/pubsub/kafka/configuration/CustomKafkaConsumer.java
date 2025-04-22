package org.chenile.pubsub.kafka.configuration;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.chenile.core.event.EventProcessor;
import org.chenile.core.model.ChenileConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Set;

/**
 * Dynamically registers Kafka listener for all topics defined in Chenile configuration.
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

    /**
     * Registers a dynamic Kafka listener after application startup.
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {

        Set<String> topics = chenileConfig.getEvents().keySet();
        if (topics.isEmpty()) {
            logger.warn("No topics found in Chenile configuration to register Kafka listeners.");
            return;
        }

        logger.info("Registering Kafka listener for topics: {}", topics);

        AbstractKafkaListenerEndpoint<String, String> endpoint = new AbstractKafkaListenerEndpoint<>() {
            @Override
            protected MessagingMessageListenerAdapter<String, String> createMessageListener(
                    MessageListenerContainer container,
                    MessageConverter messageConverter) {

                logger.info("Creating Kafka message listener for Chenile events...");

                try {
                    KafkaMessageProcessor processor = new KafkaMessageProcessor(eventProcessor);

                    Method method = KafkaMessageProcessor.class.getMethod("handleMessage", ConsumerRecord.class);
                    // Wrap the MessageListener in a MessagingMessageListenerAdapter
                    MessagingMessageListenerAdapter<String, String> adapter =
                            new RecordMessagingMessageListenerAdapter<>(processor, method, null); // No error handler

                    InvocableHandlerMethod handlerMethod = new InvocableHandlerMethod(processor, method);


                    HandlerAdapter handler = new HandlerAdapter(handlerMethod);

                    adapter.setHandlerMethod(handler);            // Optionally, set the message converter if needed
                    //adapter.setMessageConverter(messageConverter);

                    return adapter;
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        endpoint.setId("chenile_events");
        endpoint.setGroupId("chenile_events-group");
        endpoint.setTopics(topics.toArray(String[]::new));

        registry.registerListenerContainer(endpoint, factory, true);
    }

    @Override
    public int getOrder() {
        return 910;
    }

    /**
     * Processor class for handling Kafka messages.
     */
    static class KafkaMessageProcessor {
        private final EventProcessor eventProcessor;

        public KafkaMessageProcessor(EventProcessor eventProcessor) {
            this.eventProcessor = eventProcessor;
        }

        public void handleMessage(ConsumerRecord<String, String> record) {
            logger.info("Processing message from topic '{}': {}", record.topic(), record.value());
            eventProcessor.handleEvent(record.topic(), record.value());
        }
    }
}
