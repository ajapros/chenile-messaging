package org.chenile.pubsub.configuration;

import org.chenile.core.external.ExternalApiProperties;
import org.chenile.core.external.ExternalApiPublisher;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.entry.PubSubEntryPoint;
import org.chenile.pubsub.init.ChenilePubSubInitializer;
import org.chenile.pubsub.logging.PubSubExternalApiPublisher;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.chenile.pubsub.wildcard.WildCardsTopic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class PubSubConfiguration {

    @Value("${mqtt.publish.base.topic:chenile}") String basePublishTopic;
    /**
     * This is the base topic name that will pre-pended for all subscriptions. (default: chenile)
     */
    @Value("${mqtt.subscribe.base.topic:chenile}") private String baseSubscribeTopic;
    @Value("${pubsub.enabled:true}") private boolean mqttEnabled;


    @Bean
    public PubSubInfoProvider pubSubInfoProvider(){
        return new PubSubInfoProvider();
    }

    @Bean
    public ChenilePubSubInitializer chenilePubSubInitializer(WildCardsTopic wildCardsTopic){
        return new ChenilePubSubInitializer(mqttEnabled,basePublishTopic,baseSubscribeTopic,wildCardsTopic);
    }

    /**
     * A topic to service map.<br/>
     * This map is internally used to route a message that arrives at a topic to a service.<br/>
     * This map is populated  during the initialization phase.<br/>
     * It is used  during runtime to invoke the appropriate operation in a service<br/>
     * @return a configuration that maps a route to a service.
     *
     */
    @Bean
    Map<String,String> pubSubConfig(){
        return new HashMap<>();
    }

    @Bean
    public PubSubEntryPoint pubSubEntryPoint(){
        return new PubSubEntryPoint();
    }

    @Bean
    @ConditionalOnBean(ExternalApiProperties.class)
    @ConditionalOnMissingBean(ExternalApiPublisher.class)
    @ConditionalOnProperty(prefix = "chenile.external-api.logging", name = "publisher",
            havingValue = "pubsub")
    public ExternalApiPublisher externalApiPublisher(ObjectProvider<ChenilePub> chenilePubProvider,
                                                     ExternalApiProperties externalApiProperties) {
        return new PubSubExternalApiPublisher(chenilePubProvider, externalApiProperties);
    }
}
