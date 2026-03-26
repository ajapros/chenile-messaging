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
    public final List<String> handlersSeen = new CopyOnWriteArrayList<>();
    public final List<String> observations = new CopyOnWriteArrayList<>();

    public void reset(){
        this.latch = new CountDownLatch(1);
        sum =0;
        tenantsSeen.clear();
        handlersSeen.clear();
        observations.clear();
    }

    public void addTenant(String tenant) {
        tenantsSeen.add(tenant);
    }

    public void addHandler(String handler) {
        handlersSeen.add(handler);
    }

    public void addObservation(String tenant, String handler, int observedSum) {
        observations.add(tenant + "|" + handler + "|" + observedSum);
    }

    public boolean hasObservation(String tenant, String handler, int observedSum) {
        return observations.contains(tenant + "|" + handler + "|" + observedSum);
    }

    public boolean hasHandler(String handler) {
        return handlersSeen.contains(handler);
    }
}
