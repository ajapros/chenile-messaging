package org.chenile.pubsub.jvm;

import org.chenile.pubsub.jvm.model.JvmPubSubMessage;
import org.chenile.pubsub.jvm.pub.JvmPublisher;
import org.chenile.pubsub.jvm.storage.JvmPubSubStorage;
import org.chenile.pubsub.jvm.sub.JvmSubscriber;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestJvmPublisherAsyncUnit {

    @Test
    void asyncPublishReturnsBeforeSubscriberFinishes() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch subscriberStarted = new CountDownLatch(1);
            CountDownLatch unblockSubscriber = new CountDownLatch(1);
            CountDownLatch publishReturned = new CountDownLatch(1);

            BlockingSubscriber jvmSubscriber = new BlockingSubscriber(subscriberStarted, unblockSubscriber);
            CountingStorage storage = new CountingStorage();
            JvmPublisher publisher = new JvmPublisher(new PubSubInfoProvider(), jvmSubscriber, executorService, storage);

            Thread caller = new Thread(() -> {
                publisher.asyncPublish("topicA", "payloadA", Map.of("k", "v"));
                publishReturned.countDown();
            });
            caller.start();

            Assertions.assertTrue(subscriberStarted.await(1, TimeUnit.SECONDS),
                    "subscriber should start on executor thread");
            Assertions.assertTrue(publishReturned.await(200, TimeUnit.MILLISECONDS),
                    "asyncPublish should return without waiting for subscriber processing");

            unblockSubscriber.countDown();
            caller.join(2000);

            Assertions.assertEquals(1, jvmSubscriber.callCount.get());
            Assertions.assertEquals(1, storage.saveCount.get());
        } finally {
            executorService.shutdownNow();
        }
    }

    static class BlockingSubscriber extends JvmSubscriber {
        private final CountDownLatch started;
        private final CountDownLatch unblock;
        private final AtomicInteger callCount = new AtomicInteger(0);

        BlockingSubscriber(CountDownLatch started, CountDownLatch unblock) {
            super(null);
            this.started = started;
            this.unblock = unblock;
        }

        @Override
        public void onMessage(String topic, String payload, Map<String, Object> headers) {
            callCount.incrementAndGet();
            started.countDown();
            try {
                unblock.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class CountingStorage implements JvmPubSubStorage {
        private final AtomicInteger saveCount = new AtomicInteger(0);

        @Override
        public void save(JvmPubSubMessage message) {
            saveCount.incrementAndGet();
        }
    }
}
