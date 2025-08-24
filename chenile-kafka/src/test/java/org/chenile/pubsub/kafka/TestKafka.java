package org.chenile.pubsub.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.kafka.service.Payload;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringConfig.class)
@ActiveProfiles("unittest")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext
public  class TestKafka extends KafkaBaseTest {

	@Autowired private ChenilePub chenilePub;
	@Autowired private SharedData sharedData;




	@Test @Order(1) public void testIfHeadersAndPayloadWork() throws Exception {
		Payload payload = new Payload(5,8);
		Map<String, Object> headers = new HashMap<>();
		headers.put("num3",10);
		//headers.put(Constants.TEST_MODE, true);
		String s = new ObjectMapper().writeValueAsString(payload);

		chenilePub.publishToOperation("testService","f",
				s,headers);
		if(!sharedData.latch.await(1000, TimeUnit.SECONDS)){
			Assert.fail("Timed out waiting for the function to complete");
		}
		Assert.assertEquals("Sum is not computed correctly",23,sharedData.sum );
	}




	@Test
	@Order(2)
	public void testEventHeadersAndPayloadWork() throws Exception {

		sharedData.latch = new CountDownLatch(1);
		Payload payload = new Payload(5,8);
		Map<String, Object> headers = new HashMap<>();
		headers.put("num3",10);
		//headers.put(Constants.TEST_MODE, true);
		String s = new ObjectMapper().writeValueAsString(payload);

		chenilePub.asyncPublish("kafka1",
				s,headers);

		if(!sharedData.latch.await(1000, TimeUnit.SECONDS)){
			Assert.fail("Timed out waiting for the function to complete");
		}
		Assert.assertEquals("Sum is not computed correctly",15,sharedData.sum );
	}

}
