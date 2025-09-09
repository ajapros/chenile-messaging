package org.chenile.pubsub.kafka.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.chenile.pubsub.entry.PubSubEntryPoint;
import org.chenile.pubsub.kafka.constants.ChenileKafkaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerConfig.class);
    @Value(value = "${spring.kafka.bootstrap-server}")
    private String bootstrapAddress;

    @Autowired
    private PubSubEntryPoint pubSubEntryPoint;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
          bootstrapAddress);
        props.put(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
          StringDeserializer.class);
        props.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
          StringDeserializer.class);
        props.put(GROUP_ID_CONFIG,"hello2");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
          new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

   @KafkaListener(topics = ChenileKafkaConstants.CHENILE_GLOBAL_TOPIC, groupId = "foo1")
    public void listenGroupFoo2(@Payload String message, @Headers Map<String,Object> headers,
                                @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                @Header(ChenileKafkaConstants.CHENILE_TOPIC_KEY) String chenileTopic) {
        System.out.println("Received Message in group f1: " + message);
        System.out.println(headers);
        messageArrived(chenileTopic,message,headers);
    }

    public void messageArrived(String topic, String messageContent,  Map<String,Object>  headers)  {
        logger.info("Received at topic = |" + topic + "| message = ||\n" + messageContent + "||\n"
                + " with User properties = ");
        if(headers != null) {
            headers.forEach(
                    (k,v) -> {
                        logger.info("key = " + k + " value = " + new String(String.valueOf(v)));
                    });
        }

        try {
            Map<String,Object> map = new HashMap<>();
            headers.forEach(
                    (k,v) -> {
                        if (v instanceof byte[]){
                            byte[] t = (byte[]) v;
                            map.put(k , new String(t));
                        }else{
                            map.put(k ,(String.valueOf(v)));
                        }

                    });
            pubSubEntryPoint.process(topic, messageContent, map);
        }catch(Exception e){
            logger.error("Exception in entry point. Message = " + e.getMessage());
        }
    }


}