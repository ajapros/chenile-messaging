package org.chenile.pubsub.azure.service;

public interface TestService {
	int f(int num3, Payload payload);

	int f1(Payload payload);

	int orderEvent(Payload payload);

	int invoicePaid(Payload payload);

	int auditEvent(Payload payload);

	int dlHandler(Payload payload);
}
