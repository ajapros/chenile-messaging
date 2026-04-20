package org.chenile.pubsub.azure.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Component
@ConfigurationProperties(prefix = "spring.chenile.azure.eventhubs")
public class ChenileEventHubProperties {

    private String connectionString;

    private String dl;

    private List<String> producers = new ArrayList<>();

    private List<String> clients = new ArrayList<>();

    /**
     * Maps logical Chenile event names to physical Event Hub names.
     */
    private Map<String, String> routes = new HashMap<>();

    /**
     * Fallback physical Event Hub for logical topics that are not explicitly mapped.
     */
    private String defaultRoute;

    /**
     * When enabled, tenant/client identifiers are prefixed to the physical Event Hub name.
     */
    private boolean clientPrefixEnabled = false;

    private String clientPrefixSeparator = "_";

    private Consumers consumers = new Consumers();

    /** Flag to control automatic consumer start */
    private boolean autoStartConsumers = true;

    public static class Consumers {
        private Map<String, HubConfig> hubs = new HashMap<>();

        public Map<String, HubConfig> getHubs() { return hubs; }
        public void setHubs(Map<String, HubConfig> hubs) { this.hubs = hubs; }
    }

    public static class HubConfig {
        private String consumerGroup;

        public String getConsumerGroup() { return consumerGroup; }
        public void setConsumerGroup(String consumerGroup) { this.consumerGroup = consumerGroup; }
    }

    // Getters & Setters
    public String getConnectionString() { return connectionString; }
    public void setConnectionString(String connectionString) { this.connectionString = connectionString; }

    public List<String> getProducers() { return producers; }
    public void setProducers(List<String> producers) { this.producers = producers; }

    public List<String> getClients() { return clients; }
    public void setClients(List<String> clients) { this.clients = clients; }

    public Map<String, String> getRoutes() { return routes; }
    public void setRoutes(Map<String, String> routes) { this.routes = routes; }

    public String getDefaultRoute() { return defaultRoute; }
    public void setDefaultRoute(String defaultRoute) { this.defaultRoute = defaultRoute; }

    public boolean isClientPrefixEnabled() { return clientPrefixEnabled; }
    public void setClientPrefixEnabled(boolean clientPrefixEnabled) { this.clientPrefixEnabled = clientPrefixEnabled; }

    public String getClientPrefixSeparator() { return clientPrefixSeparator; }
    public void setClientPrefixSeparator(String clientPrefixSeparator) { this.clientPrefixSeparator = clientPrefixSeparator; }

    public Consumers getConsumers() { return consumers; }
    public void setConsumers(Consumers consumers) { this.consumers = consumers; }

    public void setAutoStartConsumers(boolean autoStartConsumers) {
        this.autoStartConsumers = autoStartConsumers;
    }

    public boolean isAutoStartConsumers() {
        return autoStartConsumers;
    }

    public String getDl() {
        return dl;
    }

    public void setDl(String dl) {
        this.dl = dl;
    }

    public String resolvePhysicalHubName(String hubName) {
        if (hubName == null || hubName.isBlank()) {
            return hubName;
        }
        String physicalHub = null;
        if (routes != null && !routes.isEmpty()) {
            physicalHub = routes.get(hubName);
        }
        if (physicalHub == null) {
            physicalHub = defaultRoute;
        }
        if (physicalHub == null) {
            return hubName;
        }
        if (physicalHub.isBlank()) {
            throw new IllegalStateException(
                    "Route for logical topic '" + hubName + "' points to a blank physical Event Hub name"
            );
        }
        return physicalHub;
    }

    public List<String> getResolvedProducerHubs() {
        Set<String> hubs = new LinkedHashSet<>();
        for (String producer : producers) {
            String resolved = resolvePhysicalHubName(producer);
            if (resolved != null && !resolved.isBlank()) {
                hubs.add(resolved);
            }
        }
        if (defaultRoute != null && !defaultRoute.isBlank()) {
            hubs.add(resolvePhysicalHubName("__default_route_probe__"));
        }
        if (dl != null && !dl.isBlank()) {
            hubs.add(resolvePhysicalHubName(dl));
        }
        return new ArrayList<>(hubs);
    }

    public Map<String, HubConfig> getResolvedConsumerHubs() {
        Map<String, HubConfig> resolved = new LinkedHashMap<>();
        consumers.getHubs().forEach((configuredHub, hubConfig) -> {
            String physicalHub = configuredHub;
            if (routes != null && routes.containsKey(configuredHub)) {
                physicalHub = resolvePhysicalHubName(configuredHub);
            }
            HubConfig existing = resolved.get(physicalHub);
            if (existing != null && !Objects.equals(existing.getConsumerGroup(), hubConfig.getConsumerGroup())) {
                throw new IllegalStateException(
                        "Conflicting consumer groups configured for physical hub '" + physicalHub + "'"
                );
            }
            resolved.putIfAbsent(physicalHub, hubConfig);
        });
        return resolved;
    }
}
