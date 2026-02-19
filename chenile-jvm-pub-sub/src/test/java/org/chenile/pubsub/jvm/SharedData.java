package org.chenile.pubsub.jvm;

import java.util.concurrent.CountDownLatch;

public class SharedData {
    public CountDownLatch latch = new CountDownLatch(1);
    public int sum;

    public void reset() {
        this.latch = new CountDownLatch(1);
        sum = 0;
    }
}
