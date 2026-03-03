package org.chenile.pubsub.jvm.configuration;

import org.chenile.pubsub.jvm.pub.JvmPublisher;
import org.chenile.pubsub.jvm.storage.JvmPubSubStorage;
import org.chenile.pubsub.jvm.storage.StaticMapJvmPubSubStorage;
import org.chenile.pubsub.jvm.sub.JvmSubscriber;
import org.chenile.pubsub.model.ChenilePubSub;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.chenile.pubsub.wildcard.WildCardsTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class JvmPubSubConfiguration {

    @Bean
    public JvmPublisher jvmPublisher(PubSubInfoProvider pubSubInfoProvider,
                                     JvmSubscriber jvmSubscriber,
                                     ExecutorService jvmPubSubExecutor,
                                     JvmPubSubStorage jvmPubSubStorage) {
        return new JvmPublisher(pubSubInfoProvider, jvmSubscriber, jvmPubSubExecutor, jvmPubSubStorage);
    }

    @Bean
    public JvmSubscriber jvmSubscriber(org.chenile.core.event.EventProcessor eventProcessor) {
        return new JvmSubscriber(eventProcessor);
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "jvmPubSubExecutor")
    public ExecutorService jvmPubSubExecutor() {
        return Executors.newCachedThreadPool();
    }

    @Bean
    @ConditionalOnMissingBean(JvmPubSubStorage.class)
    public JvmPubSubStorage jvmPubSubStorage() {
        return new StaticMapJvmPubSubStorage();
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
