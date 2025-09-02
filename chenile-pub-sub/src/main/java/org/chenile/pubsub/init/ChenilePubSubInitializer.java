package org.chenile.pubsub.init;

import org.chenile.base.exception.ConfigurationException;
import org.chenile.core.model.ChenileConfiguration;
import org.chenile.core.model.ChenileServiceDefinition;
import org.chenile.http.annotation.ChenileController;
import org.chenile.pubsub.entry.PubSubEntryPoint;
import org.chenile.pubsub.errorcodes.ErrorCodes;
import org.chenile.pubsub.model.ChenilePubSub;
import org.chenile.pubsub.wildcard.WildCardsTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;

import java.lang.annotation.Annotation;
import java.util.*;

/**
 * Detects the {@link ChenilePubSub} annotated classes in the Application Context and
 * populates the chenileConfig for later use by {@link}
 * Topic and Qos can be configured at the service level. (not at the operation level)<br/>
 * When a service is subscribed, all operations under the service will be subscribed automatically.
 */
public class ChenilePubSubInitializer implements InitializingBean {
    Logger logger = LoggerFactory.getLogger(ChenilePubSubInitializer.class);
    @Autowired
    ChenileConfiguration chenileConfiguration;

    @Autowired
    private PubSubEntryPoint pubSubEntryPoint;

    @Value("${pubsub.topic.separator}") private String separator;

    @Autowired
    ApplicationContext applicationContext;
    @Autowired @Qualifier("pubSubConfig")
    Map<String,String> pubSubConfig;

    final boolean pubsubEnabled;
    final WildCardsTopic wildCardsTopic;

    public ChenilePubSubInitializer(boolean enabled, String basePublishTopic, String baseSubscribeTopic, WildCardsTopic wildCardsTopic){
        this.basePublishTopicName = basePublishTopic;
        this.baseSubscribeTopicName = baseSubscribeTopic;
        this.pubsubEnabled = enabled;
        this.wildCardsTopic = wildCardsTopic;
    }
    private final String basePublishTopicName;
    private final String baseSubscribeTopicName;

    @EventListener(ApplicationReadyEvent.class)
    @Order(900) // ensure that it is called after core/http got initialized first
    public void init() throws Exception {
        Map<String,Object> beans = applicationContext.getBeansWithAnnotation(ChenilePubSub.class);
        List<String> topics = new ArrayList<>();
        // register all of these beans as Mqtt beans
        for(Map.Entry<String, Object> e: beans.entrySet()) {
            Object bean = e.getValue();
            ChenilePubSub chenilePubSub = bean.getClass().getAnnotation(ChenilePubSub.class);
            ChenileController chenileController = bean.getClass().getAnnotation(ChenileController.class);
            if (chenileController == null){
                throw new ConfigurationException(ErrorCodes.MISCONFIGURATION.getSubError(), new Object[]{e.getKey()});
            }
            String serviceId = chenileController.value();
            String publishTopic = chenilePubSub.publishTopic();
            if (publishTopic.isEmpty()){
                publishTopic = basePublishTopicName + separator + serviceId;
            }
            String subscribeTopic = chenilePubSub.subscribeTopic();
            if (subscribeTopic.isEmpty()){
                subscribeTopic = baseSubscribeTopicName + separator + serviceId;
            }
            int qos = chenilePubSub.qos();

            putAnnotationBackIntoServiceDefinition(publishTopic,subscribeTopic,qos,serviceId);
            pubSubConfig.put(subscribeTopic,serviceId);
            // subscribe to this topic and all the topics underneath it
            // subscribe to this topic and all the topics underneath it
            // We use a single level filter since all operations are supported under it
            if(!pubsubEnabled) return; // don't subscribe to the topic if mqtt is not enabled.
            // but we need to do the rest of the stuff. Otherwise, we cannot publish to the correct topic
            logger.info("Subscribing to topic " + subscribeTopic + "/+");
            wildCardsTopic.subscribeTo(subscribeTopic, chenilePubSub);


//            IMqttToken token = mqttV5Client.subscribe(subscribeTopic + "/+", qos);
//            token.waitForCompletion();


            logger.info("Subscribing to topic " + subscribeTopic );
            topics.add("^"+subscribeTopic+"_.*");

        }
        //subscribeToDynamicTopic(topics);
    }

    /**
     * Put the details of the data structure back into the service definition. <br/>
     * This is needed since the init method takes default values that are configured in the
     * annotation and mutates them.
     * @param publishTopic - the topic to publish when you want to invoke the service remotely
     * @param subscribeTopic - the topic to subscribe for the service
     * @param qos - the qos level to subscribe to
     * @param serviceId - the service Id of the service that gets mapped to the topic and qos
     */
    private void putAnnotationBackIntoServiceDefinition(String publishTopic,String subscribeTopic, int qos, String serviceId){
        ChenileServiceDefinition csd = chenileConfiguration.getServices().get(serviceId);
        ChenilePubSub chenileMqtt = new ChenilePubSub(){

            @Override
            public Class<? extends Annotation> annotationType() {
                return ChenilePubSub.class;
            }

            @Override
            public String subscribeTopic() {
                return subscribeTopic;
            }

            @Override
            public int qos() {
                return qos;
            }

            @Override
            public String publishTopic() {
                return publishTopic;
            }
        };
        csd.putExtensionAsAnnotation(ChenilePubSub.class,chenileMqtt);
    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }
}