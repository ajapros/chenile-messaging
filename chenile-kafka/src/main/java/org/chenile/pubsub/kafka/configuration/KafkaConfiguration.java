package org.chenile.pubsub.kafka.configuration;

import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.kafka.impl.KafkaPublisher;
import org.chenile.pubsub.model.ChenilePubSub;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.chenile.pubsub.wildcard.WildCardsTopic;
import org.springframework.beans.factory.annotation.Autowired;
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
    @Bean
    public WildCardsTopic wildCardsTopic(){
        return new WildCardsTopic() {
            @Override
            public void subscribeTo(String subscribeTopic, ChenilePubSub chenilePubSub) {}
            @Override
            public void globalTopic() {}
        };
    }
}
