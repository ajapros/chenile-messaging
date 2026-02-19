package org.chenile.pubsub.azure.configuration;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import org.chenile.pubsub.azure.sub.AzureEventHubSubscriber;
import org.chenile.pubsub.azure.util.EventHubNameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class EventHubStaticMappingConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventHubStaticMappingConfiguration.class);

    @Autowired
    private ChenileEventHubProperties eventHubProperties;

    @Autowired
    private BlobContainerAsyncClient blobContainerAsyncClient;


    // ===============================
    // Producer Clients
    // ===============================
    @Bean
    public Map<String, EventHubProducerClient> producerClients() {
        Map<String, EventHubProducerClient> producers = new HashMap<>();
        EventHubNameUtils.expandHubNames(
                eventHubProperties.getProducers(),
                eventHubProperties.getClients(),
                eventHubProperties.getClientPrefixSeparator()
        ).forEach((hubName) -> {
            EventHubClientBuilder builder = new EventHubClientBuilder()
                    .connectionString(eventHubProperties.getConnectionString(), hubName);
            producers.put(hubName, builder.buildProducerClient());
        });
        return producers;
    }

    // ===============================
    // Consumer Clients (Processors)
    // ===============================


    @Bean
    public Map<String, EventProcessorClient> consumerProcessors(AzureEventHubSubscriber azureEventHubSubscriber) {

        Map<String, EventProcessorClient> processors = new HashMap<>();

        // Iterate over the consumer hubs from properties
        List<String> clients = eventHubProperties.getClients();
        String separator = eventHubProperties.getClientPrefixSeparator();

        eventHubProperties.getConsumers().getHubs().forEach((hubName, hubConfig) -> {
            EventHubNameUtils.expandHubNames(
                    List.of(hubName),
                    clients,
                    separator
            ).forEach((expandedHubName) -> {
                EventProcessorClient processor = new EventProcessorClientBuilder()
                        .connectionString(eventHubProperties.getConnectionString(), expandedHubName)
                        .consumerGroup(hubConfig.getConsumerGroup())
                        .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient))
                        .processEvent(azureEventHubSubscriber) // your Consumer<EventContext>
                        .processError(EventHubStaticMappingConfiguration::processError)
                        .buildEventProcessorClient();

                processors.put(expandedHubName, processor);
            });
        });

        return processors;
    }


    // ===============================
    // Error Handler
    // ===============================
    public static void processError(ErrorContext errorContext) {
        LOGGER.error("Error in partition {}: {}",
                errorContext.getPartitionContext() != null
                        ? errorContext.getPartitionContext().getPartitionId() : "N/A",
                errorContext.getThrowable().getMessage(), errorContext.getThrowable());
    }

}
