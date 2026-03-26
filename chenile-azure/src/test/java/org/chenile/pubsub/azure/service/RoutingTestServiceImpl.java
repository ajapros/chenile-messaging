package org.chenile.pubsub.azure.service;

import org.chenile.core.context.ContextContainer;
import org.chenile.pubsub.azure.SharedData;
import org.springframework.beans.factory.annotation.Autowired;

public class RoutingTestServiceImpl implements RoutingTestService {
	@Autowired
	SharedData sharedData;

	@Override
	public int shipmentEvent(Payload payload) {
		return record("shipmentEvent", payload.num1 + payload.num2 + 400);
	}

	@Override
	public int refundEvent(Payload payload) {
		return record("refundEvent", payload.num1 + payload.num2 + 500);
	}

	@Override
	public int customerAlert(Payload payload) {
		return record("customerAlert", payload.num1 + payload.num2 + 600);
	}

	private int record(String handlerName, int sum) {
		sharedData.sum = sum;
		String tenant = ContextContainer.getInstance().getTenant();
		sharedData.addTenant(tenant);
		sharedData.addHandler(handlerName);
		sharedData.addObservation(tenant, handlerName, sum);
		sharedData.latch.countDown();
		return sum;
	}
}
