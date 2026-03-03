package org.chenile.pubsub.jvm.pub;

import org.chenile.base.exception.ServerException;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.errorcodes.ErrorCodes;
import org.chenile.pubsub.jvm.model.JvmPubSubMessage;
import org.chenile.pubsub.jvm.storage.JvmPubSubStorage;
import org.chenile.pubsub.jvm.sub.JvmSubscriber;
import org.chenile.pubsub.model.ChenilePubSub;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * JVM/in-process implementation of {@link ChenilePub}.
 * Supports synchronous publish and executor-backed asynchronous publish.
 */
public class JvmPublisher implements ChenilePub {

    private static final Logger LOGGER = LoggerFactory.getLogger(JvmPublisher.class);

    private final PubSubInfoProvider pubSubInfoProvider;
    private final JvmSubscriber jvmSubscriber;
    private final ExecutorService executorService;
    private final JvmPubSubStorage storage;

    public JvmPublisher(PubSubInfoProvider pubSubInfoProvider,
                        JvmSubscriber jvmSubscriber,
                        ExecutorService executorService,
                        JvmPubSubStorage storage) {
        this.pubSubInfoProvider = pubSubInfoProvider;
        this.jvmSubscriber = jvmSubscriber;
        this.executorService = executorService;
        this.storage = storage;
    }

    @Override
    public void publishToOperation(String service, String operationName, String payload,
                                   Map<String, Object> properties) {
        ChenilePubSub pubSubInfo = pubSubInfoProvider.obtainChenileMqtt(service);
        if (pubSubInfo == null) {
            throw new ServerException(ErrorCodes.CANNOT_FIND_TOPIC.getSubError(),
                    new Object[]{service});
        }

        String topic = substituteProperties(pubSubInfo.publishTopic(), properties);
        topic = topic + "_" + operationName;

        publish(topic, payload, properties);
    }

    @Override
    public void publish(String topic, String payload, Map<String, Object> properties) {
        handleEvent(topic, payload, properties);
    }

    /**
     * Store and dispatch the message through the configured executor so caller thread returns immediately.
     */
    @Override
    public void asyncPublish(String topic, String payload, Map<String, Object> properties) {
        Map<String, Object> headers = properties == null ? Map.of() : properties;
        store(topic, payload, headers);
        executorService.submit(() -> {
            try {
                jvmSubscriber.onMessage(topic, payload, headers);
            } catch (Exception e) {
                LOGGER.error("Error processing asynchronous JVM pub-sub message for topic {}", topic, e);
            }
        });
    }

    private void handleEvent(String topic, String payload, Map<String, Object> properties) {
        Map<String, Object> headers = properties == null ? Map.of() : properties;
        store(topic, payload, headers);
        jvmSubscriber.onMessage(topic, payload, headers);
    }

    private static String substituteProperties(String text, Map<String, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return text;
        }
        String result = text;
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            result = result.replace("{" + entry.getKey() + "}", String.valueOf(entry.getValue()));
        }
        return result;
    }

    private void store(String topic, String payload, Map<String, Object> headers) {
        storage.save(new JvmPubSubMessage(topic, payload, headers, Instant.now()));
    }
}
