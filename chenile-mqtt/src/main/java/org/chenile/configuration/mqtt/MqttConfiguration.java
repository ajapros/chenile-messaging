package org.chenile.configuration.mqtt;

import org.chenile.mqtt.pubsub.MqttPublisher;
import org.chenile.mqtt.pubsub.MqttSubscriber;
import org.chenile.pubsub.model.ChenilePubSub;
import org.chenile.pubsub.wildcard.WildCardsTopic;
import org.eclipse.paho.mqttv5.client.DisconnectedBufferOptions;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.convert.converter.Converter;

import java.util.HashMap;
import java.util.Map;

/**
 * Sets up Eclipse Paho for communicating with MQ-TT broker using configurations
 */
@Configuration
public class MqttConfiguration {
    Logger logger = LoggerFactory.getLogger(MqttConfiguration.class);
    @Value("${pubsub.mqtt.connection.ServerURIs}") private String hostURI;
    /**
     * This is the base publish topic that will be pre-pended to publish  messages.
     * This can contain specific expressions such as {tenantId} for example which
     * will be substituted by the actual tenantId at the time of publishing a message.
     * This value may not be the same as the base subscribe topic because publishing
     * happens at runtime while subscription happens during startup. Hence, subscription can contain
     * wild cards (such as +) whereas publishing may include expressions that will be
     * substituted from the headers (such as tenant Id etc.)
     * But if it is a constant expression they both can be the same. (default: chenile)
     */
    @Value("${pubsub.mqtt.publish.base.topic:chenile}") String basePublishTopic;
    /**
     * This is the base topic name that will pre-pended for all subscriptions. It can contain
     * wild cards such as + in accordance with the MQ-TT subscription rules . (default: chenile)
     */
    @Value("${pubsub.mqtt.subscribe.base.topic:chenile}") private String baseSubscribeTopic;
    @Value("${pubsub.mqtt.will.payload}") private String willPayload;
    @Value("${pubsub.mqtt.will.qos}") private int willQos;
    @Value("${pubsub.mqtt.will.retained}") private boolean willRetained;
    @Value("${pubsub.mqtt.will.topic}") private String willTopic;
    @Value("${pubsub.clientID}") private String clientID;
    @Value("${pubsub.mqtt.actionTimeout}") private int actionTimeout;
    @Value("${pubsub.enabled:true}") private boolean mqttEnabled;
    @Value("${pubsub.mqtt.connection.session.expiry}") private Long sessionExpiry;

    @Bean public Map<String,String> mqttConnectionDetails(){
        Map<String,String> cd = new HashMap<>();
        cd.put("pubsub.mqtt.connection.ServerURIs",hostURI);
        return cd;
    }

    /**
     * This converts a string to a byte array. This is required to convert the password which is
     * given in the properties file as a string to a byte array that can be set in conn opts
     * @return
     */
    @Bean
    @ConfigurationPropertiesBinding
    Converter<String,byte[]> convertStringToBytes(){
        return new Converter<String,byte[]>(){
            @Override
            public byte[] convert(String source) {
                return source.getBytes();
            }
        };
    }
    @Bean
    @ConfigurationProperties(prefix = "mqtt.connection")
    MqttConnectionOptions mqttConnectionOpts(){
        return new MqttConnectionOptions();
    }
    @Bean
    MqttMessage willMessage(@Autowired MqttConnectionOptions options){
        MqttMessage willMessage = new MqttMessage(willPayload.getBytes(), willQos, willRetained, null);
        options.setWill(willTopic,willMessage);
        return willMessage;
    }

    @Bean
    MemoryPersistence memoryPersistence(){
        return new MemoryPersistence();
    }

    @Bean
    @ConfigurationProperties(prefix = "pubsub.mqtt.disconnected.buffer")
    DisconnectedBufferOptions disconnectedBufferOptions(){
        return new DisconnectedBufferOptions();
    }

    @Bean
    MqttAsyncClient mqttV5Client(
            @Autowired @Qualifier("mqttConnectionDetails") Map<String,String> mqttConnectionDetails,
            @Autowired MqttConnectionOptions connOpts,
            @Autowired MemoryPersistence persistence,
            @Autowired DisconnectedBufferOptions disconnectedBufferOptions) throws MqttException {
        String uri = mqttConnectionDetails.get("pubsub.mqtt.connection.ServerURIs");
        MqttAsyncClient v5Client = new MqttAsyncClient(uri, clientID, persistence);
        v5Client.setBufferOpts(disconnectedBufferOptions);
        // Combination of clean start and session, broker will wait for subscriber for given time,
        // + publisher will store message in memory and publish when connection came back
        connOpts.setCleanStart(false);
        connOpts.setAutomaticReconnect(true);
        connOpts.setKeepAliveInterval(sessionExpiry.intValue());
        connOpts.setSessionExpiryInterval(sessionExpiry);
        IMqttToken token = v5Client.connect(connOpts);
        token.waitForCompletion(actionTimeout);
        logger.info("Connected to the MQTT broker with client ID = {}", clientID);
        return v5Client;
    }

    @Bean
    MqttSubscriber mqttSubscriber(@Autowired MqttAsyncClient v5Client) {
        MqttSubscriber subscriber = new MqttSubscriber(mqttEnabled);
        v5Client.setCallback(subscriber);
        return subscriber;
    }
    @Bean @ConfigurationProperties(prefix = "pubsub.mqtt.publish")
    MqttPublisher mqttPublisher(){
        return new MqttPublisher();
    }

    @Bean
    public WildCardsTopic wildCardsTopic(@Autowired MqttAsyncClient v5Client){
        return new WildCardsTopic() {
            @Override
            public void subscribeTo(String subscribeTopic, ChenilePubSub chenilePubSub) {
                // subscribe to this topic and all the topics underneath it
                // We use a single level filter since all operations are supported under it

                try {
                    if(!mqttEnabled) return; // don't subscribe to the topic if mqtt is not enabled.
                    // but we need to do the rest of the stuff. Otherwise, we cannot publish to the correct topic
                    logger.info("Subscribing to topic " + subscribeTopic + "/+");
                    IMqttToken token = v5Client.subscribe(subscribeTopic + "/+", chenilePubSub.qos());
                    token.waitForCompletion();
                } catch (MqttException e) {
                    throw new RuntimeException(e);
                }

            }

            @Override
            public void globalTopic() {

            }

        };


    }

}
