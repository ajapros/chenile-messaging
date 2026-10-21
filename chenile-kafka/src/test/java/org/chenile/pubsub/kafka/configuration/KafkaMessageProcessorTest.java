package org.chenile.pubsub.kafka.configuration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.chenile.core.context.ChenileExchange;
import org.chenile.core.event.EventProcessor;
import org.chenile.pubsub.kafka.configuration.CustomKafkaConsumer.KafkaMessageProcessor;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaMessageProcessorTest {

    @Test
    public void mapsKafkaTopicToChenileEventId() {
        EventProcessor eventProcessor = mock(EventProcessor.class);
        when(eventProcessor.handleEvent(eq("process-work"), eq("payload"), anyMap()))
                .thenReturn(List.of(new ChenileExchange()));

        KafkaMessageProcessor processor = new KafkaMessageProcessor(
                eventProcessor,
                Map.of("external-work", "process-work"));

        processor.handleMessage(new ConsumerRecord<>("external-work", 0, 0L, null, "payload"));

        verify(eventProcessor).handleEvent(eq("process-work"), eq("payload"), anyMap());
    }

    @Test
    public void propagatesSubscriberFailuresForKafkaRetry() {
        EventProcessor eventProcessor = mock(EventProcessor.class);
        ChenileExchange failedExchange = new ChenileExchange();
        failedExchange.setException(new IllegalStateException("worker callback failed"));
        when(eventProcessor.handleEvent(eq("process-work"), eq("payload"), anyMap()))
                .thenReturn(List.of(failedExchange));

        KafkaMessageProcessor processor = new KafkaMessageProcessor(
                eventProcessor,
                Map.of("process-work", "process-work"));

        assertThrows(
                IllegalStateException.class,
                () -> processor.handleMessage(
                        new ConsumerRecord<>("process-work", 0, 0L, null, "payload")));
    }

    @Test
    public void failsWhenNoChenileSubscriberHandledTheEvent() {
        EventProcessor eventProcessor = mock(EventProcessor.class);
        when(eventProcessor.handleEvent(eq("process-work"), eq("payload"), anyMap()))
                .thenReturn(List.of());

        KafkaMessageProcessor processor = new KafkaMessageProcessor(
                eventProcessor,
                Map.of("process-work", "process-work"));

        assertThrows(
                IllegalStateException.class,
                () -> processor.handleMessage(
                        new ConsumerRecord<>("process-work", 0, 0L, null, "payload")));
    }
}
