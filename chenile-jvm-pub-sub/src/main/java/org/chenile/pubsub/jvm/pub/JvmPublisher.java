package org.chenile.pubsub.jvm.pub;

import org.chenile.base.exception.ServerException;
import org.chenile.core.event.EventProcessor;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.errorcodes.ErrorCodes;
import org.chenile.pubsub.model.ChenilePubSub;
import org.chenile.pubsub.provider.PubSubInfoProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JvmPublisher implements ChenilePub {

    private final PubSubInfoProvider pubSubInfoProvider;
    private final EventProcessor eventProcessor;

    public JvmPublisher(PubSubInfoProvider pubSubInfoProvider, EventProcessor eventProcessor) {
        this.pubSubInfoProvider = pubSubInfoProvider;
        this.eventProcessor = eventProcessor;
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

    @Override
    public void asyncPublish(String topic, String payload, Map<String, Object> properties) {
        handleEvent(topic, payload, properties);
    }

    private void handleEvent(String topic, String payload, Map<String, Object> properties) {
        Map<String, String> headers = convertHeaders(properties);
        eventProcessor.handleEvent(topic, payload, headers);
    }

    private static Map<String, String> convertHeaders(Map<String, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> headers = new HashMap<>();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            headers.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        return headers;
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
}
