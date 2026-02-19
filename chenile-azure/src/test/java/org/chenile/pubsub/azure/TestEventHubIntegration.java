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

        BlobContainerClient containerClient =
                new BlobContainerClientBuilder()
                        .connectionString("UseDevelopmentStorage=true")
                        .containerName("chenilequeue")
                        .buildClient();

        containerClient.createIfNotExists();
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
    void testEventHubUnknownTopicAcme() throws JsonProcessingException {
        Payload payload = new Payload(5, 8);
        Map<String, Object> headers = new HashMap<>();
        headers.put("num3", 10);
        headers.put(HeaderUtils.TENANT_ID_KEY, TENANT_ACME);
        String s = new ObjectMapper().writeValueAsString(payload);

        // Assert that sending to an unknown topic throws IllegalStateException
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            chenilePub.asyncPublish("unknown", s, headers);
        });

        // Optionally, assert the exception message
        Assertions.assertTrue(exception.getMessage().contains("Azure Event Hub client for topic 'acme-unknown' is not registered"));

        // If sharedData.sum should not be changed in this case, assert it
        Assertions.assertEquals(0, sharedData.sum); // or whatever the expected default is
    }

    @Test
    void testEventHubUnknownTopicBeta() throws JsonProcessingException {
        Payload payload = new Payload(5, 8);
        Map<String, Object> headers = new HashMap<>();
        headers.put("num3", 10);
        headers.put(HeaderUtils.TENANT_ID_KEY, TENANT_BETA);
        String s = new ObjectMapper().writeValueAsString(payload);

        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            chenilePub.asyncPublish("unknown", s, headers);
        });

        Assertions.assertTrue(exception.getMessage().contains("Azure Event Hub client for topic 'beta-unknown' is not registered"));
        Assertions.assertEquals(0, sharedData.sum);
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

        Assertions.assertTrue(sharedData.latch.await(30, TimeUnit.SECONDS));

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

    private void sendAndAssertSum(String tenant, Payload payload, int num3, int expectedSum)
            throws JsonProcessingException, InterruptedException {
        Map<String, Object> headers = new HashMap<>();
        headers.put("num3", num3);
        headers.put(HeaderUtils.TENANT_ID_KEY, tenant);
        String s = new ObjectMapper().writeValueAsString(payload);
        chenilePub.asyncPublish("chenile", s, headers);
        System.out.println("Message sent to Event Hub: " + s);

        Assertions.assertTrue(sharedData.latch.await(30, TimeUnit.SECONDS));
        Assertions.assertEquals(expectedSum, sharedData.sum);
    }
}
