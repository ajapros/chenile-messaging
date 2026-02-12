package org.chenile.pubsub.azure.configuration;

import com.azure.messaging.eventhubs.EventHubProducerClient;
import org.chenile.core.event.EventProcessor;
import org.chenile.pubsub.azure.pub.AzurePublisher;
import org.chenile.pubsub.azure.sub.AzureEventHubSubscriber;
import org.chenile.pubsub.model.ChenilePubSub;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.chenile.pubsub.wildcard.WildCardsTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class AzurePubSubConfiguration {


    @Bean
    public AzurePublisher azurePublisher(PubSubInfoProvider pubSubInfoProvider) {
        return new AzurePublisher(pubSubInfoProvider);
    }

    @Bean
    AzureEventHubSubscriber azureEventHubSubscriber(EventProcessor eventProcessor){
        return new AzureEventHubSubscriber(eventProcessor);
    }


    @Bean
    public WildCardsTopic wildCardsTopic(){
        return new WildCardsTopic() {
            @Override
            public void subscribeTo(String serviceId, ChenilePubSub chenilePubSub) {

            }

            @Override
            public void globalTopic() {

            }
        };
    }

}
