package org.chenile.pubsub.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.chenile.core.context.HeaderUtils;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.azure.configuration.EventHubConsumerStarter;
import org.chenile.pubsub.azure.service.Payload;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootTest(classes = SpringConfig.class)
public class TestEventHubIntegration extends BaseComposeContainer{

    private static final String TENANT_ACME = "acme";
    private static final String TENANT_BETA = "beta";
    private static final AtomicBoolean CONSUMERS_STARTED = new AtomicBoolean(false);

    @Autowired
    private EventHubConsumerStarter eventHubConsumerStarter;

    @Autowired
    private ChenilePub chenilePub;

    @Autowired
    private   SharedData sharedData;

    @BeforeAll
    public static void createContainer() {
        BlobContainerClient containerClient = new BlobContainerClientBuilder()
                .connectionString("UseDevelopmentStorage=true")
                .containerName("chenilequeue")
                .buildClient();

        createContainerWithRetry(containerClient, 10, 1000);
        System.out.println("Blob container ready: chenilequeue");
    }

    @Test
    void testEventHubClientForAcme() throws InterruptedException, JsonProcessingException {
        ensureConsumersStarted();
        sendAndAssertSum(TENANT_ACME, new Payload(5, 8), 10, 15);
    }

    @Test
    void testEventHubClientForBeta() throws InterruptedException, JsonProcessingException {
        ensureConsumersStarted();
        sendAndAssertSum(TENANT_BETA, new Payload(7, 9), 10, 18);
    }

    @Test
    void testExplicitBusinessRouteForOrderCreated() throws InterruptedException, JsonProcessingException {
        ensureConsumersStarted();
        sendAndAssertTopicHandled("order-created", TENANT_ACME, new Payload(2, 3), 105, "orderEvent");
    }

    @Test
    void testExplicitBusinessRouteForOrderUpdated() throws InterruptedException, JsonProcessingException {
        ensureConsumersStarted();
        sendAndAssertTopicHandled("order-updated", TENANT_BETA, new Payload(4, 5), 109, "orderEvent");
    }

    @Test
    void testExplicitBillingRoute() throws InterruptedException, JsonProcessingException {
        ensureConsumersStarted();
        sendAndAssertTopicHandled("invoice-paid", TENANT_ACME, new Payload(1, 2), 203, "invoicePaid");
    }

    @Test
    void testDefaultRouteForAuditEvent() throws InterruptedException, JsonProcessingException {
        ensureConsumersStarted();
        sendAndAssertTopicHandled("audit-created", TENANT_BETA, new Payload(3, 4), 307, "auditEvent");
    }

    @Test
    void testSecondControllerBusinessRouteForShipmentCreated() throws InterruptedException, JsonProcessingException {
        ensureConsumersStarted();
        sendAndAssertTopicHandled("shipment-created", TENANT_ACME, new Payload(6, 7), 413, "shipmentEvent");
    }

    @Test
    void testSecondControllerBusinessRouteForShipmentUpdated() throws InterruptedException, JsonProcessingException {
        ensureConsumersStarted();
        sendAndAssertTopicHandled("shipment-updated", TENANT_BETA, new Payload(8, 1), 409, "shipmentEvent");
    }

    @Test
    void testSecondControllerBillingRouteForRefundIssued() throws InterruptedException, JsonProcessingException {
        ensureConsumersStarted();
        sendAndAssertTopicHandled("refund-issued", TENANT_ACME, new Payload(9, 2), 511, "refundEvent");
    }

    @Test
    void testSecondControllerDefaultRouteForCustomerFollowup() throws InterruptedException, JsonProcessingException {
        ensureConsumersStarted();
        sendAndAssertTopicHandled("customer-followup", TENANT_BETA, new Payload(2, 5), 607, "customerAlert");
    }

    @Test
    void testEventHubClientWithException() throws InterruptedException, JsonProcessingException {

        ensureConsumersStarted();

        Payload payload = new Payload(-1,-2);
        Map<String, Object> headers = new HashMap<>();
        headers.put("num3",10);
        headers.put(HeaderUtils.TENANT_ID_KEY, TENANT_ACME);
        String s = new ObjectMapper().writeValueAsString(payload);
        chenilePub.asyncPublish("chenile",s,headers);
        System.out.println("Message sent to Event Hub: " + s);

        Assertions.assertTrue(awaitCondition(() -> sharedData.hasHandler("dlHandler"), 30));

        //Assertions.assertEquals(15,sharedData.sum);

        System.out.println("All Done!!!!");

    }

    @BeforeEach
    void resetSharedData() {
        sharedData.reset();
    }

    private void ensureConsumersStarted() throws InterruptedException {
        if (CONSUMERS_STARTED.compareAndSet(false, true)) {
            Thread.sleep(5000);
            eventHubConsumerStarter.startConsumersManually();
            System.out.println("EventHub started!");
            Thread.sleep(2000);
        }
    }

    private static void createContainerWithRetry(BlobContainerClient containerClient, int maxAttempts, long sleepMillis) {
        RuntimeException lastException = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                containerClient.createIfNotExists();
                return;
            } catch (RuntimeException e) {
                lastException = e;
                if (attempt == maxAttempts) {
                    break;
                }
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for Azurite to become ready",
                            interruptedException);
                }
            }
        }
        throw new IllegalStateException("Azurite blob container did not become ready in time", lastException);
    }

    private void sendAndAssertSum(String tenant, Payload payload, int num3, int expectedSum)
            throws JsonProcessingException, InterruptedException {
        Map<String, Object> headers = new HashMap<>();
        headers.put("num3", num3);
        headers.put(HeaderUtils.TENANT_ID_KEY, tenant);
        String s = new ObjectMapper().writeValueAsString(payload);
        chenilePub.asyncPublish("chenile", s, headers);
        System.out.println("Message sent to Event Hub: " + s);

        Assertions.assertTrue(awaitCondition(() -> sharedData.hasObservation(tenant, "f1", expectedSum), 30));
    }

    private void sendAndAssertTopicHandled(String topic, String tenant, Payload payload, int expectedSum,
                                           String expectedHandler) throws JsonProcessingException, InterruptedException {
        Map<String, Object> headers = new HashMap<>();
        headers.put(HeaderUtils.TENANT_ID_KEY, tenant);
        String s = new ObjectMapper().writeValueAsString(payload);
        chenilePub.asyncPublish(topic, s, headers);
        System.out.println("Message sent to Event Hub for topic " + topic + ": " + s);

        Assertions.assertTrue(awaitCondition(() -> sharedData.hasObservation(tenant, expectedHandler, expectedSum), 30));
    }

    private boolean awaitCondition(BooleanSupplier condition, int timeoutSeconds) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadlineNanos) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(200);
        }
        return condition.getAsBoolean();
    }
}
