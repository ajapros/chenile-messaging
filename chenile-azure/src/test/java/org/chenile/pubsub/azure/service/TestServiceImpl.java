package org.chenile.pubsub.azure.service;

import org.chenile.core.context.ContextContainer;
import org.chenile.pubsub.azure.SharedData;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Test MQTT
 * Since MQTT is asynchronous, we need to use a countdown latch to take care of
 * co-ordinating between request and response.
 * Also, it is a fire and forget call, so we will use a common data structure to
 * validate if the service has been called and is returning correct values.
 *
 */
public class TestServiceImpl implements TestService {
	@Autowired  SharedData sharedData;
	@Override
	public int f(int num3, Payload payload) {
		int sum = payload.num1 + payload.num2 + num3;
		sharedData.sum = sum;
		sharedData.addTenant(ContextContainer.getInstance().getTenant());
		sharedData.latch.countDown();
		return sum;
	}


	@Override
	public int f1(Payload payload) {
		if(payload.num1<0){
			throw new RuntimeException("Test for exception");
		}
		int sum = payload.num1 + payload.num2 + 2;
		sharedData.sum = sum;
		sharedData.addTenant(ContextContainer.getInstance().getTenant());
		sharedData.latch.countDown();
		return sum;
	}

	@Override
	public int dlHandler(Payload payload) {
		System.out.println("I am DL handler!!");
		sharedData.latch.countDown();
		return 0;
	}
}
