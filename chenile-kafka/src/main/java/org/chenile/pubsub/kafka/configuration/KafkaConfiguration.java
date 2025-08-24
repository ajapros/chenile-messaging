package org.chenile.pubsub.kafka.configuration;

import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.kafka.impl.KafkaPublisher;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Kafka-related Spring configuration for Chenile.
 */
@Configuration
public class KafkaConfiguration {

    @Bean
    public ChenilePub chenilePub(KafkaTemplate<String, String> kafkaTemplate,
                                 PubSubInfoProvider pubSubInfoProvider) {
        return new KafkaPublisher(kafkaTemplate, pubSubInfoProvider);
    }
}
