package org.chenile.mqtt.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.utility.DockerImageName;

public class MqttBaseTest {

    private static HiveMQContainer hivemq;

    @BeforeClass
    public static void startContainer() {
        hivemq = new HiveMQContainer(
                DockerImageName.parse("hivemq/hivemq-ce:latest")
        );
        if (!hivemq.isRunning())
            hivemq.start();
    }

    @AfterClass
    public static void stopContainer() {
        if (hivemq != null) {
            hivemq.stop();
        }
    };


    static class HostProvider {
        public static String getServerURI() {
            return "tcp://" + hivemq.getHost() + ":" + hivemq.getMqttPort();
        }
    }

    @DynamicPropertySource
    static void mqttProperties(DynamicPropertyRegistry registry){
        registry.add("pubsub.mqtt.connection.ServerURIs",HostProvider::getServerURI);
    }

}
