package org.chenile.pubsub.azure.pub;

import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import org.chenile.core.context.HeaderUtils;
import org.chenile.pubsub.azure.configuration.ChenileEventHubProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AzurePublisherTest {

    @Test
    void buildHeadersHandlesNullProperties() {
        @SuppressWarnings("unchecked")
        Map<String, Object> headers = (Map<String, Object>) ReflectionTestUtils.invokeMethod(
                AzurePublisher.class,
                "buildHeaders",
                "chenile",
                null
        );

        Assertions.assertEquals("chenile", headers.get("chenile_topic"));
        Assertions.assertEquals(1, headers.size());
    }

    @Test
    void buildHeadersDoesNotMutateCallerProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("tenantId", "acme");

        @SuppressWarnings("unchecked")
        Map<String, Object> headers = (Map<String, Object>) ReflectionTestUtils.invokeMethod(
                AzurePublisher.class,
                "buildHeaders",
                "chenile",
                properties
        );

        Assertions.assertEquals("acme", headers.get("tenantId"));
        Assertions.assertEquals("chenile", headers.get("chenile_topic"));
        Assertions.assertFalse(properties.containsKey("chenile_topic"));
    }

    @Test
    void asyncPublishUsesPhysicalHubRoute() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        when(producerClient.createBatch(any())).thenReturn(batch);
        when(batch.tryAdd(any())).thenReturn(true);

        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setRoutes(Map.of("order-created", "business-events"));

        AzurePublisher publisher = new AzurePublisher(null, properties);
        ReflectionTestUtils.setField(publisher, "producerClients", Map.of("business-events", producerClient));

        publisher.asyncPublish("order-created", "{\"id\":1}", new HashMap<>());

        verify(producerClient).send(batch);
    }

    @Test
    void asyncPublishAppliesTenantPrefixAfterRouteResolution() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        when(producerClient.createBatch(any())).thenReturn(batch);
        when(batch.tryAdd(any())).thenReturn(true);

        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setRoutes(Map.of("order-created", "business-events"));
        properties.setClients(java.util.List.of("acme"));
        properties.setClientPrefixEnabled(true);
        properties.setClientPrefixSeparator("-");

        AzurePublisher publisher = new AzurePublisher(null, properties);
        ReflectionTestUtils.setField(publisher, "producerClients", Map.of("acme-business-events", producerClient));

        Map<String, Object> headers = new HashMap<>();
        headers.put(HeaderUtils.TENANT_ID_KEY, "acme");

        publisher.asyncPublish("order-created", "{\"id\":1}", headers);

        verify(producerClient).send(batch);
    }

    @Test
    void asyncPublishDoesNotApplyTenantPrefixUnlessEnabled() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        when(producerClient.createBatch(any())).thenReturn(batch);
        when(batch.tryAdd(any())).thenReturn(true);

        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setRoutes(Map.of("order-created", "business-events"));
        properties.setClients(java.util.List.of("acme"));
        properties.setClientPrefixSeparator("-");

        AzurePublisher publisher = new AzurePublisher(null, properties);
        ReflectionTestUtils.setField(publisher, "producerClients", Map.of("business-events", producerClient));

        Map<String, Object> headers = new HashMap<>();
        headers.put(HeaderUtils.TENANT_ID_KEY, "acme");

        publisher.asyncPublish("order-created", "{\"id\":1}", headers);

        verify(producerClient).send(batch);
    }

    @Test
    void asyncPublishFailsWhenRoutePointsToUnknownPhysicalHub() {
        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setRoutes(Map.of("order-created", "wrong-hub-name"));

        AzurePublisher publisher = new AzurePublisher(null, properties);
        ReflectionTestUtils.setField(publisher, "producerClients", Map.of());

        IllegalStateException exception = Assertions.assertThrows(
                IllegalStateException.class,
                () -> publisher.asyncPublish("order-created", "{\"id\":1}", Map.of())
        );

        Assertions.assertTrue(exception.getMessage().contains("wrong-hub-name"));
    }

    @Test
    void asyncPublishFailsFastForBlankRouteTarget() {
        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setRoutes(Map.of("order-created", " "));

        AzurePublisher publisher = new AzurePublisher(null, properties);
        ReflectionTestUtils.setField(publisher, "producerClients", Map.of());

        IllegalStateException exception = Assertions.assertThrows(
                IllegalStateException.class,
                () -> publisher.asyncPublish("order-created", "{\"id\":1}", Map.of())
        );

        Assertions.assertTrue(exception.getMessage().contains("order-created"));
    }

    @Test
    void asyncPublishUsesDefaultRouteForUnmappedTopic() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        when(producerClient.createBatch(any())).thenReturn(batch);
        when(batch.tryAdd(any())).thenReturn(true);

        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setDefaultRoute("shared-events");

        AzurePublisher publisher = new AzurePublisher(null, properties);
        ReflectionTestUtils.setField(publisher, "producerClients", Map.of("shared-events", producerClient));

        publisher.asyncPublish("audit-created", "{\"id\":1}", Map.of());

        verify(producerClient).send(batch);
    }
}
