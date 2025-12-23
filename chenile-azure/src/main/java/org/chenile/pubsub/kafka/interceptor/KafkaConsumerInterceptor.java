package org.chenile.pubsub.kafka.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaConsumerInterceptor implements ConsumerInterceptor<String, String> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerInterceptor.class);

    // Thread-safe counter for messages consumed
    private static final AtomicInteger messageCount = new AtomicInteger(0);

    public static int getMessageCount() {
        return messageCount.get();
    }

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            logger.info("[INTERCEPTOR] topic={}, partition={}, offset={}, key={}, value={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value());
            messageCount.incrementAndGet();  // Increment counter
        }
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        logger.info("[INTERCEPTOR] onCommit: {}", offsets);
    }

    @Override
    public void close() {
        logger.info("[INTERCEPTOR] closed");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        logger.info("[INTERCEPTOR] configured with configs: {}", configs);
    }
}
