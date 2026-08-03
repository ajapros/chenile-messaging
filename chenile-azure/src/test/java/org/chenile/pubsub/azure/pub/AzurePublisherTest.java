package org.chenile.pubsub.azure.pub;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import org.chenile.core.context.HeaderUtils;
import org.chenile.pubsub.azure.configuration.ChenileEventHubProperties;
import org.chenile.pubsub.interceptor.PubSubMessage;
import org.chenile.pubsub.interceptor.PubSubMessageInterceptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.chenile.pubsub.azure.constants.ChenileAzureConstants.CHENILE_AZURE_PARTITION_ID;
import static org.chenile.pubsub.azure.constants.ChenileAzureConstants.CHENILE_AZURE_PARTITION_KEY;
import static org.chenile.pubsub.azure.constants.ChenileAzureConstants.CHENILE_AZURE_PARTITION_MODE;
import static org.chenile.pubsub.azure.constants.ChenileAzureConstants.CHENILE_AZURE_PARTITION_MODE_AUTO;

public class AzurePublisherTest {

    @Test
    void asyncPublishDefaultsToPartitionZero() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        ArgumentCaptor<CreateBatchOptions> options = ArgumentCaptor.forClass(CreateBatchOptions.class);
        when(producerClient.createBatch(options.capture())).thenReturn(batch);
        when(batch.tryAdd(any())).thenReturn(true);

        AzurePublisher publisher = publisherFor(producerClient);
        publisher.asyncPublish("topic-a", "payload", Map.of());

        Assertions.assertEquals("0", options.getValue().getPartitionId());
        Assertions.assertNull(options.getValue().getPartitionKey());
    }

    @Test
    void asyncPublishUsesExplicitLegacyPartitionId() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        ArgumentCaptor<CreateBatchOptions> options = ArgumentCaptor.forClass(CreateBatchOptions.class);
        when(producerClient.createBatch(options.capture())).thenReturn(batch);
        when(batch.tryAdd(any())).thenReturn(true);

        AzurePublisher publisher = publisherFor(producerClient);
        publisher.asyncPublish("topic-a", "payload", Map.of(CHENILE_AZURE_PARTITION_ID, 2));

        Assertions.assertEquals("2", options.getValue().getPartitionId());
        Assertions.assertNull(options.getValue().getPartitionKey());
    }

    @Test
    void asyncPublishUsesAzureHashedBusinessPartitionKey() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        ArgumentCaptor<CreateBatchOptions> options = ArgumentCaptor.forClass(CreateBatchOptions.class);
        when(producerClient.createBatch(options.capture())).thenReturn(batch);
        when(batch.tryAdd(any())).thenReturn(true);

        AzurePublisher publisher = publisherFor(producerClient);
        publisher.asyncPublish("topic-a", "payload", Map.of(CHENILE_AZURE_PARTITION_KEY, "customer-123"));

        Assertions.assertNull(options.getValue().getPartitionId());
        Assertions.assertEquals("customer-123", options.getValue().getPartitionKey());
    }

    @Test
    void asyncPublishRejectsBothPartitionSelectors() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        AzurePublisher publisher = publisherFor(producerClient);

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> publisher.asyncPublish("topic-a", "payload", Map.of(
                        CHENILE_AZURE_PARTITION_ID, 1,
                        CHENILE_AZURE_PARTITION_KEY, "customer-123")));

        Assertions.assertTrue(exception.getMessage().contains("Only one partition selector"));
        verify(producerClient, org.mockito.Mockito.never()).createBatch(any());
    }

    @Test
    void asyncPublishFallsBackToPartitionZeroForInvalidExplicitPartitionId() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        ArgumentCaptor<CreateBatchOptions> options = ArgumentCaptor.forClass(CreateBatchOptions.class);
        when(producerClient.createBatch(options.capture())).thenReturn(batch);
        when(batch.tryAdd(any())).thenReturn(true);

        publisherFor(producerClient).asyncPublish("topic-a", "payload",
                Map.of(CHENILE_AZURE_PARTITION_ID, "not-a-number"));

        Assertions.assertEquals("0", options.getValue().getPartitionId());
        Assertions.assertNull(options.getValue().getPartitionKey());
    }

    @Test
    void asyncPublishTreatsBlankSelectorsAsDefaultPartitionZero() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        ArgumentCaptor<CreateBatchOptions> options = ArgumentCaptor.forClass(CreateBatchOptions.class);
        when(producerClient.createBatch(options.capture())).thenReturn(batch);
        when(batch.tryAdd(any())).thenReturn(true);

        publisherFor(producerClient).asyncPublish("topic-a", "payload", Map.of(
                CHENILE_AZURE_PARTITION_ID, " ",
                CHENILE_AZURE_PARTITION_KEY, " "));

        Assertions.assertEquals("0", options.getValue().getPartitionId());
        Assertions.assertNull(options.getValue().getPartitionKey());
    }

    @Test
    void asyncPublishLeavesSelectorsUnsetForExplicitAutoDistributionMode() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        ArgumentCaptor<CreateBatchOptions> options = ArgumentCaptor.forClass(CreateBatchOptions.class);
        when(producerClient.createBatch(options.capture())).thenReturn(batch);
        when(batch.tryAdd(any())).thenReturn(true);

        publisherFor(producerClient).asyncPublish("topic-a", "payload",
                Map.of(CHENILE_AZURE_PARTITION_MODE, CHENILE_AZURE_PARTITION_MODE_AUTO));

        Assertions.assertNull(options.getValue().getPartitionId());
        Assertions.assertNull(options.getValue().getPartitionKey());
    }

    @Test
    void asyncPublishRejectsAutoModeWithExplicitPartitionId() {
        assertInvalidRouting(Map.of(
                CHENILE_AZURE_PARTITION_MODE, CHENILE_AZURE_PARTITION_MODE_AUTO,
                CHENILE_AZURE_PARTITION_ID, 1));
    }

    @Test
    void asyncPublishRejectsAutoModeWithBusinessPartitionKey() {
        assertInvalidRouting(Map.of(
                CHENILE_AZURE_PARTITION_MODE, CHENILE_AZURE_PARTITION_MODE_AUTO,
                CHENILE_AZURE_PARTITION_KEY, "customer-123"));
    }

    @Test
    void asyncPublishRejectsUnsupportedPartitionMode() {
        assertInvalidRouting(Map.of(CHENILE_AZURE_PARTITION_MODE, "round-robin"));
    }

    private void assertInvalidRouting(Map<String, Object> properties) {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> publisherFor(producerClient).asyncPublish("topic-a", "payload", properties));
        verify(producerClient, org.mockito.Mockito.never()).createBatch(any());
    }

    private AzurePublisher publisherFor(EventHubProducerClient producerClient) {
        AzurePublisher publisher = new AzurePublisher(null, new ChenileEventHubProperties());
        ReflectionTestUtils.setField(publisher, "producerClients", Map.of("topic-a", producerClient));
        return publisher;
    }

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

    @Test
    void asyncPublishAppliesInterceptorsBeforeSending() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        ArgumentCaptor<EventData> eventDataCaptor = ArgumentCaptor.forClass(EventData.class);
        when(producerClient.createBatch(any())).thenReturn(batch);
        when(batch.tryAdd(eventDataCaptor.capture())).thenReturn(true);

        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        AzurePublisher publisher = new AzurePublisher(null, properties, List.of(new PubSubMessageInterceptor() {
            @Override
            public PubSubMessage beforePublish(PubSubMessage message) {
                message.setPayload("changed");
                message.getHeaders().put("added", "header");
                return message;
            }
        }));
        ReflectionTestUtils.setField(publisher, "producerClients", Map.of("topic-a", producerClient));

        publisher.asyncPublish("topic-a", "original", Map.of("existing", "value"));

        EventData eventData = eventDataCaptor.getValue();
        Assertions.assertEquals("changed", eventData.getBodyAsString());
        Assertions.assertEquals("value", eventData.getProperties().get("existing"));
        Assertions.assertEquals("header", eventData.getProperties().get("added"));
        Assertions.assertEquals("topic-a", eventData.getProperties().get("chenile_topic"));
        verify(producerClient).send(batch);
    }

    @Test
    void asyncPublishAppliesInterceptorsInConfiguredOrder() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        ArgumentCaptor<EventData> eventDataCaptor = ArgumentCaptor.forClass(EventData.class);
        when(producerClient.createBatch(any())).thenReturn(batch);
        when(batch.tryAdd(eventDataCaptor.capture())).thenReturn(true);

        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        AzurePublisher publisher = new AzurePublisher(null, properties, List.of(
                new PubSubMessageInterceptor() {
                    @Override
                    public PubSubMessage beforePublish(PubSubMessage message) {
                        message.setPayload(message.getPayload() + "-first");
                        return message;
                    }
                },
                new PubSubMessageInterceptor() {
                    @Override
                    public PubSubMessage beforePublish(PubSubMessage message) {
                        message.setPayload(message.getPayload() + "-second");
                        return message;
                    }
                }
        ));
        ReflectionTestUtils.setField(publisher, "producerClients", Map.of("topic-a", producerClient));

        publisher.asyncPublish("topic-a", "original", Map.of());

        Assertions.assertEquals("original-first-second", eventDataCaptor.getValue().getBodyAsString());
        verify(producerClient).send(batch);
    }

    @Test
    void asyncPublishFailsWhenInterceptorReturnsNull() {
        EventHubProducerClient producerClient = mock(EventHubProducerClient.class);
        EventDataBatch batch = mock(EventDataBatch.class);
        when(producerClient.createBatch(any())).thenReturn(batch);

        AzurePublisher publisher = new AzurePublisher(null, new ChenileEventHubProperties(),
                List.of(new PubSubMessageInterceptor() {
                    @Override
                    public PubSubMessage beforePublish(PubSubMessage message) {
                        return null;
                    }
                }));
        ReflectionTestUtils.setField(publisher, "producerClients", Map.of("topic-a", producerClient));

        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> publisher.asyncPublish("topic-a", "payload", Map.of()));

        Assertions.assertTrue(exception.getMessage().contains("beforePublish"));
    }
}
