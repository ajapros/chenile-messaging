package org.chenile.pubsub.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.azure.configuration.EventHubConsumerStarter;
import org.chenile.pubsub.azure.service.Payload;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;

@SpringBootTest(classes = SpringConfig.class)
public class TestEventHubIntegration extends BaseComposeContainer{

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
    void testEventHubClient() throws InterruptedException, JsonProcessingException {

        Thread.sleep(5000);
        sharedData.reset();
        eventHubConsumerStarter.startConsumersManually();
        System.out.println("EventHub started!");

        System.out.println(sharedData.latch.getCount());

        Thread.sleep(2000);

        Payload payload = new Payload(5,8);
        Map<String, Object> headers = new HashMap<>();
        headers.put("num3",10);
        String s = new ObjectMapper().writeValueAsString(payload);
        chenilePub.asyncPublish("chenile",s,headers);
        System.out.println("Message sent to Event Hub: " + s);

        sharedData.latch.await();

        Assertions.assertEquals(15,sharedData.sum);

        System.out.println("All Done!!!!");

    }

    @Test
    void testEventHubUnknownTopic() throws JsonProcessingException {
        Payload payload = new Payload(5, 8);
        Map<String, Object> headers = new HashMap<>();
        headers.put("num3", 10);
        String s = new ObjectMapper().writeValueAsString(payload);

        // Assert that sending to an unknown topic throws IllegalStateException
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            chenilePub.asyncPublish("unknown", s, headers);
        });

        // Optionally, assert the exception message
        Assertions.assertTrue(exception.getMessage().contains("Azure Event Hub client for topic 'unknown' is not registered"));

        // If sharedData.sum should not be changed in this case, assert it
        Assertions.assertEquals(0, sharedData.sum); // or whatever the expected default is
    }

    @Test
    void testEventHubClientWithException() throws InterruptedException, JsonProcessingException {

        Thread.sleep(5000);
        sharedData.reset();
        eventHubConsumerStarter.startConsumersManually();
        System.out.println("EventHub started!");

        System.out.println(sharedData.latch.getCount());

        Thread.sleep(2000);

        Payload payload = new Payload(-1,-2);
        Map<String, Object> headers = new HashMap<>();
        headers.put("num3",10);
        String s = new ObjectMapper().writeValueAsString(payload);
        chenilePub.asyncPublish("chenile",s,headers);
        System.out.println("Message sent to Event Hub: " + s);

        sharedData.latch.await();

        //Assertions.assertEquals(15,sharedData.sum);

        System.out.println("All Done!!!!");

    }

}
