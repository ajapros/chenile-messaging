package org.chenile.pubsub.kafka.configuration;

import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.entry.PubSubEntryPoint;
import org.chenile.pubsub.init.ChenilePubSubInitializer;
import org.chenile.pubsub.kafka.impl.KafkaPublisher;
import org.chenile.pubsub.kafka.provider.PubSubInfoProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {


    @Value("${mqtt.publish.base.topic:chenile}") String basePublishTopic;
    /**
     * This is the base topic name that will pre-pended for all subscriptions. It can contain
     * wild cards such as + in accordance with the MQ-TT subscription rules . (default: chenile)
     */
    @Value("${mqtt.subscribe.base.topic:chenile}") private String baseSubscribeTopic;
    @Value("${mqtt.enabled:true}") private boolean mqttEnabled;


    @Bean
    Map<String,String> pubSubConfig(){
        return new HashMap<>();
    }

    @Bean
    public PubSubInfoProvider pubSubInfoProvider(){
        return new PubSubInfoProvider();
    }

    @Bean
    public ChenilePubSubInitializer chenilePubSubInitializer(){
        return new ChenilePubSubInitializer(mqttEnabled,basePublishTopic,baseSubscribeTopic);
    }

    @Bean
    public ChenilePub chenilePub(){
        return new KafkaPublisher();
    }

    @Bean
    public PubSubEntryPoint pubSubEntryPoint(){
        return new PubSubEntryPoint();
    }
}
