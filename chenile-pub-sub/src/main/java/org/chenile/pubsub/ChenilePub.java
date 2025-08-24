package org.chenile.pubsub;


import java.util.Map;

public interface ChenilePub {

    void publishToOperation(String service, String operationName, String payload, Map<String,Object> properties);

    void publish(String topic,  String payload, Map<String,Object> properties);

    void asyncPublish(String topic, String payload, Map<String,Object> properties);

}
