package org.chenile.pubsub.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.chenile.core.context.LogRecord;
import org.chenile.core.external.ExternalApiDirection;
import org.chenile.core.external.ExternalApiProperties;
import org.chenile.core.external.ExternalApiPublisher;
import org.chenile.pubsub.ChenilePub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Map;

public class PubSubExternalApiPublisher implements ExternalApiPublisher {
    private static final Logger logger = LoggerFactory.getLogger(PubSubExternalApiPublisher.class);

    private final ObjectProvider<ChenilePub> chenilePubProvider;
    private final ExternalApiProperties properties;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PubSubExternalApiPublisher(ObjectProvider<ChenilePub> chenilePubProvider,
                                      ExternalApiProperties properties) {
        this.chenilePubProvider = chenilePubProvider;
        this.properties = properties;
    }

    @Override
    public void publish(LogRecord record) {
        String topic = properties.topic(record.direction == LogRecord.Direction.INBOUND
                ? ExternalApiDirection.INBOUND : ExternalApiDirection.OUTBOUND);
        if (topic == null || topic.isBlank()) {
            return;
        }
        ChenilePub chenilePub = chenilePubProvider.getIfAvailable();
        if (chenilePub == null) {
            return;
        }
        try {
            chenilePub.asyncPublish(topic, objectMapper.writeValueAsString(record), Map.of());
        } catch (Exception e) {
            logger.warn("Unable to publish external API record to topic {}", topic, e);
        }
    }
}
