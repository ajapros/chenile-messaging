package org.chenile.pubsub.jvm.service;

import org.chenile.pubsub.jvm.SharedData;
import org.springframework.beans.factory.annotation.Autowired;

public class TestServiceImpl implements TestService {
    @Autowired
    SharedData sharedData;

    @Override
    public int f(int num3, Payload payload) {
        int sum = payload.num1 + payload.num2 + num3;
        sharedData.sum = sum;
        sharedData.latch.countDown();
        return sum;
    }

    @Override
    public int f1(Payload payload) {
        if (payload.num1 < 0) {
            throw new RuntimeException("Test for exception");
        }
        int sum = payload.num1 + payload.num2 + 2;
        sharedData.sum = sum;
        sharedData.latch.countDown();
        return sum;
    }

    @Override
    public int dlHandler(Payload payload) {
        sharedData.latch.countDown();
        return 0;
    }
}
