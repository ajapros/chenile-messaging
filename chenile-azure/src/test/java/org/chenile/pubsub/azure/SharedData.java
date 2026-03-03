package org.chenile.pubsub.azure;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Shared data structure that is updated by the server and checked by the
 * test class. It contains a countdown latch that will be updated when the
 * server is done so the test class can check
 */
public class SharedData {
    public CountDownLatch latch = new CountDownLatch(1);
    public int sum; // this will be updated with the computed sum by the test class.
    public final List<String> tenantsSeen = new CopyOnWriteArrayList<>();

    public void reset(){
        this.latch = new CountDownLatch(1);
        sum =0;
        tenantsSeen.clear();
    }

    public void addTenant(String tenant) {
        tenantsSeen.add(tenant);
    }
}
