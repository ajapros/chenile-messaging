package org.chenile.pubsub.jvm.storage;

import org.chenile.pubsub.jvm.model.JvmPubSubMessage;

import java.util.List;

/**
 * Storage abstraction for JVM pub-sub messages.
 * Applications can override the default implementation by providing their own bean.
 */
public interface JvmPubSubStorage {

    /**
     * Persist an incoming or outgoing pub-sub message.
     */
    void save(JvmPubSubMessage message);

    /**
     * Retrieve stored messages for a topic.
     */
    default List<JvmPubSubMessage> findByTopic(String topic) {
        return List.of();
    }

    /**
     * Clear storage (primarily useful for tests and in-memory implementations).
     */
    default void clear() {
    }
}
