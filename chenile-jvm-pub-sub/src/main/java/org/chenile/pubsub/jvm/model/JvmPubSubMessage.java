package org.chenile.pubsub.jvm.model;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public record JvmPubSubMessage(String topic,
                               String payload,
                               Map<String, Object> headers,
                               Instant createdAt) {

    public JvmPubSubMessage {
        headers = headers == null ? Map.of() : Collections.unmodifiableMap(new HashMap<>(headers));
        createdAt = createdAt == null ? Instant.now() : createdAt;
    }
}
