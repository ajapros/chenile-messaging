package org.chenile.pubsub.azure.configuration;

import com.azure.messaging.eventhubs.EventHubProducerClient;
import org.chenile.core.context.ChenileExchange;
import org.chenile.core.context.ContextContainer;
import org.chenile.core.event.EventProcessor;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.ChenileMessageHandler;
import org.chenile.pubsub.azure.pub.AzurePublisher;
import org.chenile.pubsub.azure.sub.AzureEventHubSubscriber;
import org.chenile.pubsub.interceptor.PubSubMessageInterceptor;
import org.chenile.pubsub.model.ChenilePubSub;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.chenile.pubsub.wildcard.WildCardsTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class AzurePubSubConfiguration {


    @Bean
    public AzurePublisher azurePublisher(PubSubInfoProvider pubSubInfoProvider,
                                         ChenileEventHubProperties chenileEventHubProperties,
                                         List<PubSubMessageInterceptor> pubSubMessageInterceptors) {
        return new AzurePublisher(pubSubInfoProvider, chenileEventHubProperties, pubSubMessageInterceptors);
    }

    @Bean
    AzureEventHubSubscriber azureEventHubSubscriber(EventProcessor eventProcessor,
                                                    ChenilePub chenilePub,
                                                    ChenileEventHubProperties chenileEventHubProperties,
                                                    List<PubSubMessageInterceptor> pubSubMessageInterceptors,
                                                    List<ChenileMessageHandler> messageHandlers){
        return new AzureEventHubSubscriber(eventProcessor,chenilePub,chenileEventHubProperties,
                pubSubMessageInterceptors, messageHandlers);
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
