package org.chenile.pubsub.jvm.service;

public interface TestService {
    int f(int num3, Payload payload);

    int f1(Payload payload);

    int dlHandler(Payload payload);
}
