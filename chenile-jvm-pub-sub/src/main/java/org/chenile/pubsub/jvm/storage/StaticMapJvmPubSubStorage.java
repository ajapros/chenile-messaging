package org.chenile.pubsub.jvm.storage;

import org.chenile.pubsub.jvm.model.JvmPubSubMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class StaticMapJvmPubSubStorage implements JvmPubSubStorage {

    private static final Map<String, List<JvmPubSubMessage>> STORAGE = new ConcurrentHashMap<>();

    @Override
    public void save(JvmPubSubMessage message) {
        if (message == null || message.topic() == null) {
            return;
        }
        STORAGE.computeIfAbsent(message.topic(), ignored -> new CopyOnWriteArrayList<>())
                .add(message);
    }

    @Override
    public List<JvmPubSubMessage> findByTopic(String topic) {
        if (topic == null) {
            return List.of();
        }
        return new ArrayList<>(STORAGE.getOrDefault(topic, List.of()));
    }

    @Override
    public void clear() {
        STORAGE.clear();
    }
}
