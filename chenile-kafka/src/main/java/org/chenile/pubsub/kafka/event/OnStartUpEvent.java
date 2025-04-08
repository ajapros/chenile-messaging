package org.chenile.pubsub.kafka.event;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.core.KafkaTemplate;

//@Component
public class OnStartUpEvent implements ApplicationListener<ContextRefreshedEvent> {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    int counter =1;
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {

        while (true){

            kafkaTemplate.send("chenile_",String.valueOf(++counter));

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }

    }
}