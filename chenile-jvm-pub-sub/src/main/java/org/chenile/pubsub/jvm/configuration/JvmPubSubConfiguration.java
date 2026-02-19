package org.chenile.pubsub.jvm.configuration;

import org.chenile.core.event.EventProcessor;
import org.chenile.pubsub.jvm.pub.JvmPublisher;
import org.chenile.pubsub.model.ChenilePubSub;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.chenile.pubsub.wildcard.WildCardsTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JvmPubSubConfiguration {

    @Bean
    public JvmPublisher jvmPublisher(PubSubInfoProvider pubSubInfoProvider,
                                     EventProcessor eventProcessor) {
        return new JvmPublisher(pubSubInfoProvider, eventProcessor);
    }

    @Bean
    public WildCardsTopic wildCardsTopic() {
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
