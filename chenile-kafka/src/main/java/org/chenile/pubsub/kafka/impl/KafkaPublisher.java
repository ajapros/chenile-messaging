package org.chenile.pubsub.kafka.impl;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.chenile.base.exception.ServerException;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.errorcodes.ErrorCodes;
import org.chenile.pubsub.kafka.provider.PubSubInfoProvider;
import org.chenile.pubsub.model.ChenilePubSub;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.chenile.pubsub.kafka.constants.ChenileKafkaConstants.CHENILE_GLOBAL_TOPIC;
import static org.chenile.pubsub.kafka.constants.ChenileKafkaConstants.CHENILE_TOPIC_KEY;

public class KafkaPublisher implements ChenilePub {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @Autowired
    private PubSubInfoProvider pubSubInfoProvider;

    @Override
    public void publishToOperation(String service, String operationName, String payload, Map<String, Object> properties) {

        ChenilePubSub m = pubSubInfoProvider.obtainChenileMqtt(service);
        if (m == null) {
            throw new ServerException(ErrorCodes.CANNOT_FIND_TOPIC.getSubError(),
                    new Object[] { service});
        }
        String topic = substituteProperties(m.publishTopic(),properties);
        topic = topic + "_" + operationName;
        publish(topic,payload,properties);

    }

    @Override
    public void publish(String topic, String payload, Map<String, Object> properties) {

        List<Header> headers = new ArrayList<>();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            headers.add(new RecordHeader(entry.getKey(), String.valueOf(entry.getValue()).getBytes(StandardCharsets.UTF_8)));
        }
        headers.add(new RecordHeader(CHENILE_TOPIC_KEY,String.valueOf(topic).getBytes(StandardCharsets.UTF_8)));

        ProducerRecord<String, String> record = new ProducerRecord <>(CHENILE_GLOBAL_TOPIC, null, "message", payload, headers);


        kafkaTemplate.send(record);
    }


    /**
     * Substitute the property in the place holder.
     * @param s the string with placeholders
     * @param properties the properties that need to be substituted
     * @return the s
     */
    private static String substituteProperties(String s, Map<String,Object> properties){
        for(String prop: properties.keySet()){
            s = s.replaceAll("\\{"+ prop +"}",properties.get(prop).toString());
        }
        return s;
    }
}
