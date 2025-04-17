package org.chenile.pubsub.configuration;

import org.chenile.pubsub.entry.PubSubEntryPoint;
import org.chenile.pubsub.init.ChenilePubSubInitializer;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class PubSubConfiguration {

    @Value("${mqtt.publish.base.topic:chenile}") String basePublishTopic;
    /**
     * This is the base topic name that will pre-pended for all subscriptions. It can contain
     * wild cards such as + in accordance with the MQ-TT subscription rules . (default: chenile)
     */
    @Value("${mqtt.subscribe.base.topic:chenile}") private String baseSubscribeTopic;
    @Value("${mqtt.enabled:true}") private boolean mqttEnabled;


    @Bean
    public PubSubInfoProvider pubSubInfoProvider(){
        return new PubSubInfoProvider();
    }

    @Bean
    public ChenilePubSubInitializer chenilePubSubInitializer(){
        return new ChenilePubSubInitializer(mqttEnabled,basePublishTopic,baseSubscribeTopic);
    }

    @Bean
    Map<String,String> pubSubConfig(){
        return new HashMap<>();
    }

    @Bean
    public PubSubEntryPoint pubSubEntryPoint(){
        return new PubSubEntryPoint();
    }

}
