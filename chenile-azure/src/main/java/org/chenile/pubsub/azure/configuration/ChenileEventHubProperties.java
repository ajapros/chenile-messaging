package org.chenile.pubsub.azure.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "spring.chenile.azure.eventhubs")
public class ChenileEventHubProperties {

    private String connectionString;

    private String dl;

    private List<String> producers = new ArrayList<>();

    private List<String> clients = new ArrayList<>();

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
}
