package org.chenile.pubsub.jvm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.chenile.core.context.HeaderUtils;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.jvm.storage.JvmPubSubStorage;
import org.chenile.pubsub.jvm.service.Payload;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = SpringConfig.class)
public class TestJvmPubSubIntegration {

    @Autowired
    private ChenilePub chenilePub;

    @Autowired
    private SharedData sharedData;

    @Autowired
    private JvmPubSubStorage jvmPubSubStorage;

    @BeforeEach
    void resetSharedData() {
        sharedData.reset();
        jvmPubSubStorage.clear();
    }

    @Test
    void testPublishToEvent() throws InterruptedException, JsonProcessingException {
        Payload payload = new Payload(5, 8);
        Map<String, Object> headers = new HashMap<>();
        headers.put("num3", 10);
        headers.put(HeaderUtils.TENANT_ID_KEY, "acme");
        String body = new ObjectMapper().writeValueAsString(payload);

        chenilePub.asyncPublish("chenile", body, headers);

        Assertions.assertTrue(sharedData.latch.await(5, TimeUnit.SECONDS));
        Assertions.assertEquals(15, sharedData.sum);
        Assertions.assertEquals(1, jvmPubSubStorage.findByTopic("chenile").size());
        Assertions.assertEquals("acme", sharedData.tenantsSeen.getFirst());
    }

    @Test
    void testPublishToOperation() throws InterruptedException, JsonProcessingException {
        Payload payload = new Payload(2, 3);
        Map<String, Object> headers = new HashMap<>();
        headers.put("num3", 4);
        headers.put(HeaderUtils.TENANT_ID_KEY, "tenant-op");
        String body = new ObjectMapper().writeValueAsString(payload);

        chenilePub.publishToOperation("testService", "f", body, headers);

        Assertions.assertTrue(sharedData.latch.await(5, TimeUnit.SECONDS));
        Assertions.assertEquals(9, sharedData.sum);
        Assertions.assertEquals(1, jvmPubSubStorage.findByTopic("chenile_testService_f").size());
        Assertions.assertEquals("tenant-op", sharedData.tenantsSeen.getFirst());
    }

    @Test
    void testTenantContextDoesNotLeakAcrossMessages() throws Exception {
        sharedData.reset(2);
        jvmPubSubStorage.clear();

        Payload payloadWithTenant = new Payload(1, 2);
        Payload payloadWithoutTenant = new Payload(3, 4);

        Map<String, Object> withTenantHeaders = new HashMap<>();
        withTenantHeaders.put(HeaderUtils.TENANT_ID_KEY, "acme");

        chenilePub.publish("chenile",
                new ObjectMapper().writeValueAsString(payloadWithTenant),
                withTenantHeaders);
        chenilePub.publish("chenile",
                new ObjectMapper().writeValueAsString(payloadWithoutTenant),
                Map.of());

        Assertions.assertTrue(sharedData.latch.await(5, TimeUnit.SECONDS));
        Assertions.assertEquals("acme", sharedData.tenantsSeen.get(0));
        Assertions.assertTrue(sharedData.tenantsSeen.get(1) == null || sharedData.tenantsSeen.get(1).isBlank());
        Assertions.assertEquals(2, jvmPubSubStorage.findByTopic("chenile").size());
    }

    @Test
    void testUnknownEventDoesNotCrash() {
        chenilePub.asyncPublish("unknown-event", "payload", Map.of());
        Assertions.assertEquals(0, sharedData.sum);
    }
}
