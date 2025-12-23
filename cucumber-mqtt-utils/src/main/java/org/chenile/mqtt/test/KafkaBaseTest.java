package org.chenile.mqtt.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class KafkaBaseTest {


    protected static KafkaContainer kafkaContainer;

    @BeforeClass
    public static void startKafka() {
        kafkaContainer =
                new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
                        .withExposedPorts(9093)
                        .waitingFor(Wait.forListeningPort())
                        .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL", "PLAINTEXT")
                        .withEnv("KAFKA_LISTENER_PORT", "9093")
                        .withEnv("KAFKA_ZOOKEEPER_CONNECT", "localhost:2181");

        kafkaContainer.start();

        System.setProperty(
                "spring.kafka.bootstrap-servers",
                kafkaContainer.getHost() + ":" + kafkaContainer.getFirstMappedPort()
        );
    }

    @AfterClass
    public static void stopKafka() {
        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }
    }

    static class HostProvider {
        public static String getServerURI() {
            return kafkaContainer.getHost() + ":" + kafkaContainer.getFirstMappedPort();
        }
    }

    @DynamicPropertySource
    static void mqttProperties(DynamicPropertyRegistry registry){
        registry.add("spring.kafka.bootstrap-server",HostProvider::getServerURI);
    }

}