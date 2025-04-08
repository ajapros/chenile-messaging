package org.chenile.pubsub.kafka.provider;

import org.chenile.core.model.ChenileConfiguration;
import org.chenile.core.model.ChenileServiceDefinition;
import org.chenile.pubsub.model.ChenilePubSub;
import org.springframework.beans.factory.annotation.Autowired;

public class PubSubInfoProvider {

    @Autowired
    private ChenileConfiguration chenileConfiguration;

    public ChenilePubSub obtainChenileMqtt(String serviceId){
        ChenileServiceDefinition csd = chenileConfiguration.getServices().get(serviceId);
        return csd.getExtensionAsAnnotation(ChenilePubSub.class);
    }
}