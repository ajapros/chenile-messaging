package org.chenile.pubsub.kafka.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "chenile.kafka.consumer")
public class KafkaConsumerProperties {
    private Map<String, String> topicBindings = new LinkedHashMap<>();
    private String groupId = "chenile_events-group";
    private long retryIntervalMs = 1000;
    private long maxRetries = 3;
    private String deadLetterSuffix = ".DLT";

    public Map<String, String> getTopicBindings() {
        return topicBindings;
    }

    public void setTopicBindings(Map<String, String> topicBindings) {
        this.topicBindings = topicBindings;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public long getRetryIntervalMs() {
        return retryIntervalMs;
    }

    public void setRetryIntervalMs(long retryIntervalMs) {
        this.retryIntervalMs = retryIntervalMs;
    }

    public long getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(long maxRetries) {
        this.maxRetries = maxRetries;
    }

    public String getDeadLetterSuffix() {
        return deadLetterSuffix;
    }

    public void setDeadLetterSuffix(String deadLetterSuffix) {
        this.deadLetterSuffix = deadLetterSuffix;
    }
}
