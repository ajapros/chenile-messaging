package org.chenile.pubsub.azure.sub;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.messaging.eventhubs.models.PartitionContext;
import org.chenile.base.exception.ErrorNumException;
import org.chenile.core.context.ChenileExchange;
import org.chenile.core.event.EventProcessor;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.azure.configuration.ChenileEventHubProperties;
import org.chenile.pubsub.interceptor.PubSubMessage;
import org.chenile.pubsub.interceptor.PubSubMessageInterceptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AzureEventHubSubscriberTest {

    @Test
    void processDeadLettersPublishesExceptionResults() {
        RecordingChenilePub chenilePub = new RecordingChenilePub();
        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setDl("eh2");
        AzureEventHubSubscriber subscriber = new AzureEventHubSubscriber(
                new NoOpEventProcessor(),
                chenilePub,
                properties
        );

        ChenileExchange exchange = new ChenileExchange();
        exchange.setException(new ErrorNumException(100, "boom"));
        Map<String, Object> originalProperties = new HashMap<>();
        originalProperties.put("tenantId", "acme");

        subscriber.processDeadLetters("payload", originalProperties, List.of(exchange));

        Assertions.assertEquals("eh2", chenilePub.topic);
        Assertions.assertEquals("payload", chenilePub.payload);
        Assertions.assertEquals("boom", chenilePub.properties.get("e"));
        Assertions.assertEquals("acme", chenilePub.properties.get("tenantId"));
        Assertions.assertFalse(originalProperties.containsKey("e"));
    }

    @Test
    void processDeadLettersPropagatesPublishFailures() {
        ChenileEventHubProperties properties = new ChenileEventHubProperties();
        properties.setDl("eh2");
        AzureEventHubSubscriber subscriber = new AzureEventHubSubscriber(
                new NoOpEventProcessor(),
                new FailingChenilePub(),
                properties
        );

        ChenileExchange exchange = new ChenileExchange();
        exchange.setException(new ErrorNumException(100, "boom"));

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () ->
                subscriber.processDeadLetters("payload", Map.of(), List.of(exchange)));

        Assertions.assertEquals("dl publish failed", exception.getMessage());
    }

    @Test
    void acceptAppliesInterceptorsBeforeProcessing() {
        RecordingEventProcessor processor = new RecordingEventProcessor();
        AzureEventHubSubscriber subscriber = new AzureEventHubSubscriber(
                processor,
                new RecordingChenilePub(),
                new ChenileEventHubProperties(),
                List.of(new PubSubMessageInterceptor() {
                    @Override
                    public PubSubMessage beforeSubscribe(PubSubMessage message) {
                        message.setPayload("changed");
                        message.getHeaders().put("added", "header");
                        return message;
                    }
                })
        );

        EventData eventData = new EventData("original");
        eventData.getProperties().put("chenile_topic", "topic-a");
        EventContext eventContext = mock(EventContext.class);
        when(eventContext.getEventData()).thenReturn(eventData);
        when(eventContext.getPartitionContext()).thenReturn(new PartitionContext("namespace", "eventhub", "$Default", "0"));

        subscriber.accept(eventContext);

        Assertions.assertEquals("topic-a", processor.topic);
        Assertions.assertEquals("changed", processor.payload);
        Assertions.assertEquals("header", processor.headers.get("added"));
        verify(eventContext).updateCheckpoint();
    }

    @Test
    void acceptAppliesInterceptorsInConfiguredOrder() {
        RecordingEventProcessor processor = new RecordingEventProcessor();
        AzureEventHubSubscriber subscriber = new AzureEventHubSubscriber(
                processor,
                new RecordingChenilePub(),
                new ChenileEventHubProperties(),
                List.of(
                        new PubSubMessageInterceptor() {
                            @Override
                            public PubSubMessage beforeSubscribe(PubSubMessage message) {
                                message.setPayload(message.getPayload() + "-first");
                                return message;
                            }
                        },
                        new PubSubMessageInterceptor() {
                            @Override
                            public PubSubMessage beforeSubscribe(PubSubMessage message) {
                                message.setPayload(message.getPayload() + "-second");
                                return message;
                            }
                        }
                )
        );

        EventContext eventContext = eventContext("topic-a", "original");

        subscriber.accept(eventContext);

        Assertions.assertEquals("original-first-second", processor.payload);
        verify(eventContext).updateCheckpoint();
    }

    @Test
    void acceptFailsWhenInterceptorReturnsNull() {
        AzureEventHubSubscriber subscriber = new AzureEventHubSubscriber(
                new NoOpEventProcessor(),
                new RecordingChenilePub(),
                new ChenileEventHubProperties(),
                List.of(new PubSubMessageInterceptor() {
                    @Override
                    public PubSubMessage beforeSubscribe(PubSubMessage message) {
                        return null;
                    }
                })
        );
        EventContext eventContext = eventContext("topic-a", "payload");

        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> subscriber.accept(eventContext));

        Assertions.assertTrue(exception.getMessage().contains("beforeSubscribe"));
        verify(eventContext, never()).updateCheckpoint();
    }

    static class NoOpEventProcessor extends EventProcessor {
        @Override
        public List<ChenileExchange> handleEvent(String topic, Object payload, Map<String, String> headers) {
            return Collections.emptyList();
        }
    }

    static class RecordingEventProcessor extends EventProcessor {
        private String topic;
        private Object payload;
        private Map<String, String> headers;

        @Override
        public List<ChenileExchange> handleEvent(String topic, Object payload, Map<String, String> headers) {
            this.topic = topic;
            this.payload = payload;
            this.headers = headers;
            return Collections.emptyList();
        }
    }

    static class RecordingChenilePub implements ChenilePub {
        private String topic;
        private String payload;
        private Map<String, Object> properties;

        @Override
        public void publishToOperation(String service, String operationName, String payload, Map<String, Object> properties) {
        }

        @Override
        public void publish(String topic, String payload, Map<String, Object> properties) {
        }

        @Override
        public void asyncPublish(String topic, String payload, Map<String, Object> properties) {
            this.topic = topic;
            this.payload = payload;
            this.properties = properties;
        }
    }

    static class FailingChenilePub implements ChenilePub {
        @Override
        public void publishToOperation(String service, String operationName, String payload, Map<String, Object> properties) {
        }

        @Override
        public void publish(String topic, String payload, Map<String, Object> properties) {
        }

        @Override
        public void asyncPublish(String topic, String payload, Map<String, Object> properties) {
            throw new RuntimeException("dl publish failed");
        }
    }

    private static EventContext eventContext(String topic, String payload) {
        EventData eventData = new EventData(payload);
        eventData.getProperties().put("chenile_topic", topic);
        EventContext eventContext = mock(EventContext.class);
        when(eventContext.getEventData()).thenReturn(eventData);
        when(eventContext.getPartitionContext()).thenReturn(new PartitionContext("namespace", "eventhub", "$Default", "0"));
        return eventContext;
    }
}
