package org.chenile.pubsub.jvm;

import org.chenile.core.context.ChenileExchange;
import org.chenile.core.context.ContextContainer;
import org.chenile.core.context.HeaderUtils;
import org.chenile.core.event.EventProcessor;
import org.chenile.pubsub.jvm.sub.JvmSubscriber;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class TestJvmSubscriberTenantTest {

    @Test
    void setsTenantForSubscriberReceiverAndRestoresPreviousTenant() {
        RecordingEventProcessor processor = new RecordingEventProcessor();
        JvmSubscriber subscriber = new JvmSubscriber(processor);

        ContextContainer.getInstance().setTenant("existing");
        subscriber.onMessage("topicA", "payloadA", Map.of(HeaderUtils.TENANT_ID_KEY, "acme"));

        Assertions.assertEquals(List.of("acme"), processor.tenantsSeen);
        Assertions.assertEquals("existing", ContextContainer.getInstance().getTenant());
    }

    @Test
    void doesNotLeakTenantWhenHeaderMissing() {
        RecordingEventProcessor processor = new RecordingEventProcessor();
        JvmSubscriber subscriber = new JvmSubscriber(processor);

        ContextContainer.getInstance().setTenant("seed");
        subscriber.onMessage("topicA", "payloadA", Map.of());

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
}
