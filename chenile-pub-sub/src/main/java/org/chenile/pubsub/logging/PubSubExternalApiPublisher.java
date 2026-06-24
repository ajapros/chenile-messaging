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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

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
        if (record == null) {
            return;
        }
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
            chenilePub.asyncPublish(topic, payload(record), Map.of());
        } catch (Exception e) {
            logger.warn("Unable to publish external API record to topic {}", topic, e);
        }
    }

    private String payload(LogRecord record) throws Exception {
        LogRecord safeRecord = safeRecord(record);
        try {
            return objectMapper.writeValueAsString(safeRecord);
        } catch (Exception e) {
            logger.warn("Unable to serialize full external API record. Publishing fallback record. errorType={} message={}",
                    e.getClass().getName(), e.getMessage());
            return fallbackPayload(safeRecord, e);
        }
    }

    private LogRecord safeRecord(LogRecord record) {
        LogRecord safeRecord = new LogRecord();
        safeRecord.success = record.success;
        safeRecord.responseMessages = record.responseMessages;
        safeRecord.serviceName = record.serviceName;
        safeRecord.operationName = record.operationName;
        safeRecord.moduleName = record.moduleName;
        safeRecord.headers = safeHeaders(record.headers);
        safeRecord.request = record.request;
        safeRecord.response = record.response;
        safeRecord.originalSource = record.originalSource;
        safeRecord.originalSourceReference = record.originalSourceReference;
        safeRecord.exception = record.exception;
        safeRecord.direction = record.direction;
        safeRecord.external = record.external;
        safeRecord.externalSystem = record.externalSystem;
        safeRecord.externalOperation = record.externalOperation;
        safeRecord.protocol = record.protocol;
        safeRecord.target = record.target;
        safeRecord.httpMethod = record.httpMethod;
        safeRecord.httpStatusCode = record.httpStatusCode;
        safeRecord.durationMillis = record.durationMillis;
        safeRecord.timestamp = record.timestamp;
        safeRecord.requestId = record.requestId;
        safeRecord.correlationId = record.correlationId;
        safeRecord.requestPayload = record.requestPayload;
        safeRecord.responsePayload = record.responsePayload;
        safeRecord.errorCode = record.errorCode;
        safeRecord.errorMessage = record.errorMessage;
        return safeRecord;
    }

    private Map<String, Object> safeHeaders(Map<String, Object> headers) {
        Map<String, Object> safeHeaders = new LinkedHashMap<>();
        if (headers == null || headers.isEmpty()) {
            return safeHeaders;
        }
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            try {
                String key = entry.getKey();
                if (key == null || key.isBlank()) {
                    continue;
                }
                Optional<Object> value = safeHeaderValue(key, entry.getValue());
                value.ifPresent(safeValue -> safeHeaders.put(key, safeValue));
            } catch (Exception e) {
                logger.debug("Skipping external API log header because it cannot be sanitized", e);
            }
        }
        return safeHeaders;
    }

    private Optional<Object> safeHeaderValue(String key, Object value) {
        if (value == null) {
            return Optional.empty();
        }
        if (value instanceof String || value instanceof Number || value instanceof Boolean) {
            return Optional.of(value);
        }
        if (value instanceof Character character) {
            return Optional.of(String.valueOf(character));
        }
        if (value instanceof Enum<?> enumValue) {
            return Optional.of(enumValue.name());
        }
        logger.debug("Skipping external API log header {} because value type {} is not primitive-like",
                key, value.getClass().getName());
        return Optional.empty();
    }

    private String fallbackPayload(LogRecord record, Exception serializationException) throws Exception {
        Map<String, Object> fallback = new LinkedHashMap<>();
        fallback.put("success", record.success);
        fallback.put("serviceName", record.serviceName);
        fallback.put("operationName", record.operationName);
        fallback.put("moduleName", record.moduleName);
        fallback.put("headers", record.headers);
        fallback.put("direction", record.direction == null ? null : record.direction.name());
        fallback.put("external", record.external);
        fallback.put("externalSystem", record.externalSystem);
        fallback.put("externalOperation", record.externalOperation);
        fallback.put("protocol", record.protocol);
        fallback.put("target", record.target);
        fallback.put("httpMethod", record.httpMethod);
        fallback.put("httpStatusCode", record.httpStatusCode);
        fallback.put("durationMillis", record.durationMillis);
        fallback.put("timestamp", record.timestamp);
        fallback.put("requestId", record.requestId);
        fallback.put("correlationId", record.correlationId);
        fallback.put("requestPayload", record.requestPayload);
        fallback.put("responsePayload", record.responsePayload);
        fallback.put("errorCode", record.errorCode);
        fallback.put("errorMessage", record.errorMessage);
        fallback.put("serializationError", serializationException.getClass().getName());
        fallback.put("serializationErrorMessage", serializationException.getMessage());
        return objectMapper.writeValueAsString(fallback);
    }
}
