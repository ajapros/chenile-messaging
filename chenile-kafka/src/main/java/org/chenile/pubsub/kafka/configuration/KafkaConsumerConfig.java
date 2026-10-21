package org.chenile.pubsub.kafka.configuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.chenile.pubsub.entry.PubSubEntryPoint;
import org.chenile.pubsub.kafka.constants.ChenileKafkaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.backoff.FixedBackOff;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

@EnableKafka
@Configuration
@EnableConfigurationProperties(KafkaConsumerProperties.class)
public class KafkaConsumerConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${spring.kafka.bootstrap-servers:${spring.kafka.bootstrap-server:localhost:9092}}")
    private String bootstrapAddress;

    @Value("${spring.kafka.consumer.interceptor:}")
    private String interceptorClass;

    @Autowired
    private PubSubEntryPoint pubSubEntryPoint;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(GROUP_ID_CONFIG, "chenile_group");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if (!interceptorClass.isEmpty()) {
            props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorClass);
        }
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaConsumerProperties consumerProperties) {
        validate(consumerProperties);

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(
                        record.topic() + consumerProperties.getDeadLetterSuffix(),
                        -1));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(10));

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(
                        consumerProperties.getRetryIntervalMs(),
                        consumerProperties.getMaxRetries()));
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    private void validate(KafkaConsumerProperties consumerProperties) {
        if (consumerProperties.getRetryIntervalMs() < 0 || consumerProperties.getMaxRetries() < 0) {
            throw new IllegalArgumentException("Kafka retry interval and max retries must be non-negative");
        }
        String deadLetterSuffix = consumerProperties.getDeadLetterSuffix();
        if (deadLetterSuffix == null || deadLetterSuffix.isBlank()) {
            throw new IllegalArgumentException("Kafka dead-letter topic suffix must not be blank");
        }
    }

    @KafkaListener(topics = ChenileKafkaConstants.CHENILE_GLOBAL_TOPIC, groupId = "foo1")
    public void listenGroupFoo2(
            @Payload String message,
            @Headers Map<String, Object> headers,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(ChenileKafkaConstants.CHENILE_TOPIC_KEY) String chenileTopic) {
        messageArrived(chenileTopic, message, headers);
    }

    public void messageArrived(String topic, String messageContent, Map<String, Object> headers) {
        logger.debug("Received message from global Kafka topic {}: {}", topic, messageContent);
        Map<String, Object> messageHeaders = new HashMap<>();
        if (headers != null) {
            headers.forEach((key, value) -> {
                if (value instanceof byte[] bytes) {
                    messageHeaders.put(key, new String(bytes));
                } else {
                    messageHeaders.put(key, String.valueOf(value));
                }
            });
        }

        try {
            pubSubEntryPoint.process(topic, messageContent, messageHeaders);
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Kafka message processing failed for topic '" + topic + "'",
                    exception);
        }
    }
}
