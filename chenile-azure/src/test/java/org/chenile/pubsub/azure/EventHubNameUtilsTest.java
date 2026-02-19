package org.chenile.pubsub.azure;

import org.chenile.core.context.HeaderUtils;
import org.chenile.pubsub.azure.util.EventHubNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventHubNameUtilsTest {

    @Test
    void resolveHubNameWithClientPrefix() {
        Map<String, Object> headers = new HashMap<>();
        headers.put(HeaderUtils.TENANT_ID_KEY, "acme");

        String resolved = EventHubNameUtils.resolveHubName(
                "chenile",
                headers,
                List.of("acme", "beta"),
                "-"
        );

        Assertions.assertEquals("acme-chenile", resolved);
    }

    @Test
    void resolveHubNameWithoutMatchingClient() {
        Map<String, Object> headers = new HashMap<>();
        headers.put(HeaderUtils.TENANT_ID_KEY, "unknown");

        String resolved = EventHubNameUtils.resolveHubName(
                "chenile",
                headers,
                List.of("acme", "beta"),
                "-"
        );

        Assertions.assertEquals("chenile", resolved);
    }

    @Test
    void resolveHubNameWhenClientsNotConfigured() {
        Map<String, Object> headers = new HashMap<>();
        headers.put(HeaderUtils.TENANT_ID_KEY, "acme");

        String resolved = EventHubNameUtils.resolveHubName(
                "chenile",
                headers,
                List.of(),
                "-"
        );

        Assertions.assertEquals("chenile", resolved);
    }

    @Test
    void expandHubNamesWithClients() {
        List<String> hubs = List.of("chenile", "eh2");

        List<String> expanded = EventHubNameUtils.expandHubNames(
                hubs,
                List.of("acme", "beta"),
                "-"
        );

        Assertions.assertEquals(
                List.of("acme-chenile", "acme-eh2", "beta-chenile", "beta-eh2"),
                expanded
        );
    }

    @Test
    void expandHubNamesWithoutClients() {
        List<String> hubs = List.of("chenile", "eh2");

        List<String> expanded = EventHubNameUtils.expandHubNames(
                hubs,
                List.of(),
                "-"
        );

        Assertions.assertEquals(hubs, expanded);
    }
}
