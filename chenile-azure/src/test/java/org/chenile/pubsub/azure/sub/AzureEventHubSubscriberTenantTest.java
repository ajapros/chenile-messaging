package org.chenile.pubsub.azure.sub;

import org.chenile.core.context.ChenileExchange;
import org.chenile.core.context.ContextContainer;
import org.chenile.core.context.HeaderUtils;
import org.chenile.core.event.EventProcessor;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.azure.configuration.ChenileEventHubProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class AzureEventHubSubscriberTenantTest {

    @Test
    void setsTenantForProcessingAndRestoresPreviousTenant() {
        RecordingEventProcessor processor = new RecordingEventProcessor();
        AzureEventHubSubscriber subscriber = new AzureEventHubSubscriber(
                processor,
                new NoOpChenilePub(),
                new ChenileEventHubProperties()
        );

        ContextContainer.getInstance().setTenant("existing");
        subscriber.processWithTenantContext("chenile", "payload",
                Map.of(HeaderUtils.TENANT_ID_KEY, "acme"));

        Assertions.assertEquals(List.of("acme"), processor.tenantsSeen);
        Assertions.assertEquals("existing", ContextContainer.getInstance().getTenant());
    }

    @Test
    void doesNotLeakTenantWhenHeaderMissing() {
        RecordingEventProcessor processor = new RecordingEventProcessor();
        AzureEventHubSubscriber subscriber = new AzureEventHubSubscriber(
                processor,
                new NoOpChenilePub(),
                new ChenileEventHubProperties()
        );

        ContextContainer.getInstance().setTenant("seed");
        subscriber.processWithTenantContext("chenile", "payload", Map.of());

        String seen = processor.tenantsSeen.getFirst();
        Assertions.assertTrue(seen == null || seen.isBlank());
        Assertions.assertEquals("seed", ContextContainer.getInstance().getTenant());
    }

    static class RecordingEventProcessor extends EventProcessor {
        private final List<String> tenantsSeen = new CopyOnWriteArrayList<>();

        @Override
        public List<ChenileExchange> handleEvent(String topic, Object payload, Map<String, String> headers) {
            tenantsSeen.add(ContextContainer.getInstance().getTenant());
            return Collections.emptyList();
        }
    }

    static class NoOpChenilePub implements ChenilePub {
        @Override
        public void publishToOperation(String service, String operationName, String payload, Map<String, Object> properties) {
        }

        @Override
        public void publish(String topic, String payload, Map<String, Object> properties) {
        }

        @Override
        public void asyncPublish(String topic, String payload, Map<String, Object> properties) {
        }
    }
}
