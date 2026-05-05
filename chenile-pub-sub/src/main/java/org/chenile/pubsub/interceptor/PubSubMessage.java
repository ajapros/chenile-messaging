package org.chenile.pubsub.interceptor;

import java.util.HashMap;
import java.util.Map;

public class PubSubMessage {
    private String topic;
    private String payload;
    private Map<String, Object> headers = new HashMap<>();
    private PubSubDirection direction;
    private String provider;
    private Map<String, Object> metadata = new HashMap<>();

    public PubSubMessage() {
    }

    public PubSubMessage(String topic, String payload, Map<String, Object> headers,
                         PubSubDirection direction, String provider, Map<String, Object> metadata) {
        this.topic = topic;
        this.payload = payload;
        setHeaders(headers);
        this.direction = direction;
        this.provider = provider;
        setMetadata(metadata);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers == null ? new HashMap<>() : new HashMap<>(headers);
    }

    public PubSubDirection getDirection() {
        return direction;
    }

    public void setDirection(PubSubDirection direction) {
        this.direction = direction;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata == null ? new HashMap<>() : new HashMap<>(metadata);
    }
}
