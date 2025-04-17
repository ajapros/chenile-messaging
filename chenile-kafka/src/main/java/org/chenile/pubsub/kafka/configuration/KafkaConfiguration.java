package org.chenile.pubsub.kafka.configuration;

import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.kafka.impl.KafkaPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class KafkaConfiguration {

    @Bean
    public ChenilePub chenilePub(){
        return new KafkaPublisher();
    }

}
