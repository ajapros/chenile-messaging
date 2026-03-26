package org.chenile.pubsub.azure.service;

public interface RoutingTestService {
	int shipmentEvent(Payload payload);

	int refundEvent(Payload payload);

	int customerAlert(Payload payload);
}
