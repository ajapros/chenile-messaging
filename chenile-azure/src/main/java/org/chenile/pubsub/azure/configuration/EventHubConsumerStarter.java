package org.chenile.pubsub.azure.configuration;

import com.azure.messaging.eventhubs.EventProcessorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class EventHubConsumerStarter {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventHubConsumerStarter.class);

    private final Map<String, EventProcessorClient> consumerProcessors;
    private final boolean autoStart;

    public EventHubConsumerStarter(Map<String, EventProcessorClient> consumerProcessors,
                                   EventHubProperties eventHubProperties) {
        this.consumerProcessors = consumerProcessors;
        this.autoStart = eventHubProperties.isAutoStartConsumers();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        if (!autoStart) {
            LOGGER.info("Auto-start for Event Hub consumers is disabled by configuration.");
            return;
        }

        LOGGER.info("Auto-start enabled. Starting all Event Hub consumers...");

        try {
            consumerProcessors.forEach((hub, processor) -> {
                LOGGER.info("Starting consumer for hub '{}'", hub);
                processor.start();
                LOGGER.info("Started consumer for hub '{}'", hub);
            });
        } catch (Exception e) {
            LOGGER.error("Failed to start Event Hub consumers. Stopping application.", e);
            // Fail-fast: terminate Spring Boot startup
            throw new RuntimeException("Failed to start Event Hub consumers", e);
        }

        LOGGER.info("All auto-start Event Hub consumers have been started successfully.");
    }

    /** Optional: manual start method */
    public void startConsumersManually() {
        consumerProcessors.forEach((hub, processor) -> {
            try {
                LOGGER.info("Manually starting consumer for hub '{}'", hub);
                processor.start();
                LOGGER.info("Manually started consumer for hub '{}'", hub);
            } catch (Exception e) {
                LOGGER.error("Failed to manually start consumer for hub '{}'", hub, e);
                // If desired, fail-fast for manual start as well
                throw new RuntimeException("Failed to manually start consumer for hub " + hub, e);
            }
        });
    }
}
