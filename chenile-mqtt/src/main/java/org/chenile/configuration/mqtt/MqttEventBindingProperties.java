package org.chenile.configuration.mqtt;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Binds MQTT topics to Chenile events (prefix {@code chenile.mqtt}). A message that arrives on a bound topic
 * raises the Chenile event, so every operation subscribed to it runs, exactly as for Kafka, Azure Event Hub and
 * JVM pub-sub. Bindings are opt-in: with none configured, MQTT carries only {@code @ChenilePubSub} invocations.
 * <pre>
 * chenile:
 *   mqtt:
 *     qos: 1
 *     topic-bindings:
 *       "[devices/telemetry]": telemetryReceived
 * </pre>
 * Topics are matched exactly; MQTT wildcards are not supported in bindings.
 */
public class MqttEventBindingProperties {
    private Map<String, String> topicBindings = new LinkedHashMap<>();
    private int qos = 1;

    public Map<String, String> getTopicBindings() {
        return topicBindings;
    }

    public void setTopicBindings(Map<String, String> topicBindings) {
        this.topicBindings = topicBindings;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }
}
