package org.chenile.pubsub.jvm;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class SharedData {
    public CountDownLatch latch = new CountDownLatch(1);
    public int sum;
    public final List<String> tenantsSeen = new CopyOnWriteArrayList<>();

    public void reset() {
        reset(1);
    }

    public void reset(int count) {
        this.latch = new CountDownLatch(count);
        sum = 0;
        tenantsSeen.clear();
    }

    public void addTenant(String tenant) {
        tenantsSeen.add(tenant);
    }
}
