package org.chenile.pubsub.logging;

import org.chenile.core.context.LogRecord;
import org.chenile.core.external.ExternalApiProperties;
import org.chenile.pubsub.ChenilePub;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PubSubExternalApiPublisherTest {

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
    public void skipsPublishWhenTopicIsMissing() {
        ChenilePub chenilePub = mock(ChenilePub.class);
        PubSubExternalApiPublisher publisher = new PubSubExternalApiPublisher(provider(chenilePub),
                new ExternalApiProperties(true, "", "external.out", 65536, ""));
        LogRecord record = new LogRecord();
        record.direction = LogRecord.Direction.INBOUND;

        publisher.publish(record);

        verify(chenilePub, never()).asyncPublish(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    private ObjectProvider<ChenilePub> provider(ChenilePub chenilePub) {
        ObjectProvider<ChenilePub> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(chenilePub);
        return provider;
    }
}
