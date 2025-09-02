package org.chenile.pubsub.wildcard;

import org.chenile.pubsub.model.ChenilePubSub;

public interface WildCardsTopic {

    void subscribeTo(String serviceId, ChenilePubSub chenilePubSub);

    void globalTopic();

}
