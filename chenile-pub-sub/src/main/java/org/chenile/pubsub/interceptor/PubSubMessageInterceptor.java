package org.chenile.pubsub.interceptor;

public interface PubSubMessageInterceptor {
    default PubSubMessage beforePublish(PubSubMessage message) {
        return message;
    }

    default PubSubMessage beforeSubscribe(PubSubMessage message) {
        return message;
    }
}
