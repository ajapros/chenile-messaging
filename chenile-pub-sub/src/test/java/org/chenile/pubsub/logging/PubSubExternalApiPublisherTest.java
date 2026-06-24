package org.chenile.pubsub.logging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.chenile.core.context.LogRecord;
import org.chenile.core.external.ExternalApiProperties;
import org.chenile.pubsub.ChenilePub;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PubSubExternalApiPublisherTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void publishesInboundRecordToInboundTopic() {
        ChenilePub chenilePub = mock(ChenilePub.class);
        ObjectProvider<ChenilePub> provider = provider(chenilePub);
        PubSubExternalApiPublisher publisher = new PubSubExternalApiPublisher(provider,
                new ExternalApiProperties(true, "external.in", "external.out", 65536, ""));
        LogRecord record = new LogRecord();
        record.direction = LogRecord.Direction.INBOUND;
        record.externalSystem = "partner";

        publisher.publish(record);

        verify(chenilePub).asyncPublish(eq("external.in"), any(String.class), eq(Map.of()));
    }

    @Test
    public void publishesOnlyPrimitiveLikeHeaders() throws Exception {
        ChenilePub chenilePub = mock(ChenilePub.class);
        PubSubExternalApiPublisher publisher = new PubSubExternalApiPublisher(provider(chenilePub),
                new ExternalApiProperties(true, "external.in", "external.out", 65536, ""));
        LogRecord record = inboundRecord();
        record.headers.put("string", "value");
        record.headers.put("integer", 10);
        record.headers.put("long", 20L);
        record.headers.put("boolean", true);
        record.headers.put("character", 'x');
        record.headers.put("enum", SampleEnum.ACTIVE);
        record.headers.put("springObject", new Object());
        record.headers.put("list", List.of("a", "b"));
        record.headers.put("nested", Map.of("a", "b"));

        publisher.publish(record);

        JsonNode headers = publishedPayload(chenilePub).get("headers");
        assertEquals("value", headers.get("string").asText());
        assertEquals(10, headers.get("integer").asInt());
        assertEquals(20L, headers.get("long").asLong());
        assertTrue(headers.get("boolean").asBoolean());
        assertEquals("x", headers.get("character").asText());
        assertEquals("ACTIVE", headers.get("enum").asText());
        assertFalse(headers.has("springObject"));
        assertFalse(headers.has("list"));
        assertFalse(headers.has("nested"));
    }

    @Test
    public void skipsBadHeaderAndPublishesRemainingHeaders() throws Exception {
        ChenilePub chenilePub = mock(ChenilePub.class);
        PubSubExternalApiPublisher publisher = new PubSubExternalApiPublisher(provider(chenilePub),
                new ExternalApiProperties(true, "external.in", "external.out", 65536, ""));
        LogRecord record = inboundRecord();
        record.headers = new BadHeaderMap();

        publisher.publish(record);

        JsonNode headers = publishedPayload(chenilePub).get("headers");
        assertEquals("good-value", headers.get("good").asText());
        assertFalse(headers.has("bad"));
    }

    @Test
    public void publishesFallbackPayloadWhenFullRecordCannotBeSerialized() throws Exception {
        ChenilePub chenilePub = mock(ChenilePub.class);
        PubSubExternalApiPublisher publisher = new PubSubExternalApiPublisher(provider(chenilePub),
                new ExternalApiProperties(true, "external.in", "external.out", 65536, ""));
        LogRecord record = inboundRecord();
        record.serviceName = "order-service";
        record.operationName = "create";
        record.requestId = "request-1";
        record.correlationId = "correlation-1";
        record.headers.put("tenantId", "acme");
        record.request = new Object();

        publisher.publish(record);

        JsonNode payload = publishedPayload(chenilePub);
        assertEquals("order-service", payload.get("serviceName").asText());
        assertEquals("create", payload.get("operationName").asText());
        assertEquals("request-1", payload.get("requestId").asText());
        assertEquals("correlation-1", payload.get("correlationId").asText());
        assertEquals("acme", payload.get("headers").get("tenantId").asText());
        assertTrue(payload.has("serializationError"));
        assertFalse(payload.has("request"));
    }

    @Test
    public void skipsPublishWhenTopicIsMissing() {
        ChenilePub chenilePub = mock(ChenilePub.class);
        PubSubExternalApiPublisher publisher = new PubSubExternalApiPublisher(provider(chenilePub),
                new ExternalApiProperties(true, "", "external.out", 65536, ""));
        LogRecord record = new LogRecord();
        record.direction = LogRecord.Direction.INBOUND;

        publisher.publish(record);

        verify(chenilePub, never()).asyncPublish(any(), any(), any());
    }

    private LogRecord inboundRecord() {
        LogRecord record = new LogRecord();
        record.direction = LogRecord.Direction.INBOUND;
        record.externalSystem = "partner";
        return record;
    }

    private JsonNode publishedPayload(ChenilePub chenilePub) throws Exception {
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(chenilePub).asyncPublish(eq("external.in"), payloadCaptor.capture(), eq(Map.of()));
        return objectMapper.readTree(payloadCaptor.getValue());
    }

    @SuppressWarnings("unchecked")
    private ObjectProvider<ChenilePub> provider(ChenilePub chenilePub) {
        ObjectProvider<ChenilePub> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(chenilePub);
        return provider;
    }

    private enum SampleEnum {
        ACTIVE
    }

    private static class BadHeaderMap extends AbstractMap<String, Object> {
        @Override
        public Set<Entry<String, Object>> entrySet() {
            return Set.of(
                    new SimpleEntry<>("good", "good-value"),
                    new Entry<>() {
                        @Override
                        public String getKey() {
                            return "bad";
                        }

                        @Override
                        public Object getValue() {
                            throw new IllegalStateException("bad header");
                        }

                        @Override
                        public Object setValue(Object value) {
                            throw new UnsupportedOperationException();
                        }
                    }
            );
        }
    }
}
