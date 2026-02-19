package org.chenile.pubsub.azure.util;

import org.chenile.core.context.HeaderUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class EventHubNameUtils {

    private static final String DEFAULT_SEPARATOR = "_";

    private EventHubNameUtils() {
    }

    public static String resolveHubName(String hubName,
                                        Map<String, Object> headers,
                                        List<String> clients,
                                        String separator) {
        if (hubName == null) {
            return null;
        }
        if (headers == null || headers.isEmpty()) {
            return hubName;
        }

        Object tenantObj = headers.get(HeaderUtils.TENANT_ID_KEY);
        if (tenantObj == null) {
            return hubName;
        }

        String tenant = String.valueOf(tenantObj).trim();
        if (tenant.isEmpty()) {
            return hubName;
        }

        if (clients == null || clients.isEmpty()) {
            return hubName;
        }

        if (!clients.contains(tenant)) {
            return hubName;
        }

        String sep = normalizeSeparator(separator);
        return tenant + sep + hubName;
    }

    public static List<String> expandHubNames(Collection<String> baseHubNames,
                                              List<String> clients,
                                              String separator) {
        if (baseHubNames == null || baseHubNames.isEmpty()) {
            return List.of();
        }

        if (clients == null || clients.isEmpty()) {
            return new ArrayList<>(baseHubNames);
        }

        String sep = normalizeSeparator(separator);
        Set<String> expanded = new LinkedHashSet<>();
        for (String client : clients) {
            if (client == null || client.isBlank()) {
                continue;
            }
            for (String hub : baseHubNames) {
                if (hub == null || hub.isBlank()) {
                    continue;
                }
                expanded.add(client + sep + hub);
            }
        }
        return new ArrayList<>(expanded);
    }

    private static String normalizeSeparator(String separator) {
        if (separator == null || separator.isBlank()) {
            return DEFAULT_SEPARATOR;
        }
        return separator;
    }
}
