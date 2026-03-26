package org.chenile.pubsub.azure.configuration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ChenileEventHubPropertiesTest {

    @Test
    void resolvesLogicalTopicsToDistinctProducerHubs() {
        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setProducers(List.of("order-created", "order-updated", "audit"));
        properties.setDl("order-failed");
        properties.setRoutes(Map.of(
                "order-created", "business-events",
                "order-updated", "business-events",
                "order-failed", "error-events"
        ));

        Assertions.assertEquals(
                List.of("business-events", "audit", "error-events"),
                properties.getResolvedProducerHubs()
        );
    }

    @Test
    void resolvesConsumerHubsFromRoutesAndDeduplicatesSamePhysicalHub() {
        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        ChenileEventHubProperties.HubConfig consumerGroup = new ChenileEventHubProperties.HubConfig();
        consumerGroup.setConsumerGroup("cg1");
        properties.getConsumers().setHubs(Map.of(
                "order-created", consumerGroup,
                "order-updated", consumerGroup,
                "audit-events", consumerGroup
        ));
        properties.setRoutes(Map.of(
                "order-created", "business-events",
                "order-updated", "business-events"
        ));

        Map<String, ChenileEventHubProperties.HubConfig> resolved = properties.getResolvedConsumerHubs();

        Assertions.assertEquals(Set.of("business-events", "audit-events"), resolved.keySet());
        Assertions.assertEquals("cg1", resolved.get("business-events").getConsumerGroup());
    }

    @Test
    void rejectsConflictingConsumerGroupsForSamePhysicalHub() {
        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        ChenileEventHubProperties.HubConfig cg1 = new ChenileEventHubProperties.HubConfig();
        cg1.setConsumerGroup("cg1");
        ChenileEventHubProperties.HubConfig cg2 = new ChenileEventHubProperties.HubConfig();
        cg2.setConsumerGroup("cg2");
        properties.getConsumers().setHubs(Map.of(
                "order-created", cg1,
                "order-updated", cg2
        ));
        properties.setRoutes(Map.of(
                "order-created", "business-events",
                "order-updated", "business-events"
        ));

        IllegalStateException exception = Assertions.assertThrows(
                IllegalStateException.class,
                properties::getResolvedConsumerHubs
        );

        Assertions.assertTrue(exception.getMessage().contains("business-events"));
    }

    @Test
    void fallsBackToLogicalTopicWhenRouteIsNotConfigured() {
        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setRoutes(Map.of("order-created", "business-events"));

        Assertions.assertEquals("audit-events", properties.resolvePhysicalHubName("audit-events"));
    }

    @Test
    void usesDefaultRouteWhenLogicalTopicIsNotConfigured() {
        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setRoutes(Map.of("order-created", "business-events"));
        properties.setDefaultRoute("shared-events");

        Assertions.assertEquals("shared-events", properties.resolvePhysicalHubName("audit-events"));
    }

    @Test
    void doesNotApplyDefaultRouteToExplicitConsumerHubConfiguration() {
        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        ChenileEventHubProperties.HubConfig business = new ChenileEventHubProperties.HubConfig();
        business.setConsumerGroup("cg1");
        ChenileEventHubProperties.HubConfig billing = new ChenileEventHubProperties.HubConfig();
        billing.setConsumerGroup("cg2");
        properties.getConsumers().setHubs(Map.of(
                "business-events", business,
                "billing-events", billing
        ));
        properties.setDefaultRoute("shared-events");

        Map<String, ChenileEventHubProperties.HubConfig> resolved = properties.getResolvedConsumerHubs();

        Assertions.assertEquals(Set.of("business-events", "billing-events"), resolved.keySet());
    }

    @Test
    void rejectsBlankPhysicalHubInRouteConfiguration() {
        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setRoutes(Map.of("order-created", " "));

        IllegalStateException exception = Assertions.assertThrows(
                IllegalStateException.class,
                () -> properties.resolvePhysicalHubName("order-created")
        );

        Assertions.assertTrue(exception.getMessage().contains("order-created"));
    }

    @Test
    void rejectsBlankDefaultRouteConfiguration() {
        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setDefaultRoute(" ");

        IllegalStateException exception = Assertions.assertThrows(
                IllegalStateException.class,
                () -> properties.resolvePhysicalHubName("audit-events")
        );

        Assertions.assertTrue(exception.getMessage().contains("audit-events"));
    }
}
