package org.chenile.pubsub.jvm.sub;

import org.chenile.core.context.ContextContainer;
import org.chenile.core.context.HeaderUtils;
import org.chenile.core.event.EventProcessor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * In-process subscriber that forwards events to the Chenile {@link EventProcessor}.
 * It also applies tenant context from headers for the duration of message processing.
 */
public class JvmSubscriber {

    private final EventProcessor eventProcessor;

    public JvmSubscriber(EventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    /**
     * Handle a received message, map headers to strings, and invoke the event processor.
     */
    public void onMessage(String topic, String payload, Map<String, Object> headers) {
        ContextContainer contextContainer = ContextContainer.getInstance();
        String previousTenant = contextContainer.getTenant();
        String tenant = HeaderUtils.getTenant(headers);
        if (tenant != null && tenant.isBlank()) {
            tenant = null;
        }
        try {
            // Ensure receiver-side tenant context is derived from incoming headers.
            contextContainer.setTenant(tenant);
            Map<String, String> convertedHeaders = convertHeaders(headers);
            eventProcessor.handleEvent(topic, payload, convertedHeaders);
        } finally {
            contextContainer.setTenant(previousTenant);
        }
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
}
