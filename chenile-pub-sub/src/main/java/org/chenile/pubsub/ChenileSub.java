package org.chenile.pubsub;

import java.util.Map;

public interface ChenileSub {

    void messageArrived(String topic,String message,  Map<String, Object> headers);

}
