package org.chenile.pubsub.azure.sub;

import org.chenile.base.exception.ErrorNumException;
import org.chenile.core.context.ChenileExchange;
import org.chenile.core.event.EventProcessor;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.azure.configuration.ChenileEventHubProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    static class NoOpEventProcessor extends EventProcessor {
        @Override
        public List<ChenileExchange> handleEvent(String topic, Object payload, Map<String, String> headers) {
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
}
