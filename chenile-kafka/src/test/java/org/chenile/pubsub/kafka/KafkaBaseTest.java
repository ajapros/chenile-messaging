package org.chenile.pubsub.kafka;

import org.junit.ClassRule;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaBaseTest {

    @ClassRule
    public static KafkaContainer kafkaContainer
            = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
            .withExposedPorts(9093)  // Expose port 9093 for external access
            .waitingFor(org.testcontainers.containers.wait.strategy.Wait.forListeningPort())  // Wait for the port to be ready
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL", "PLAINTEXT")
            .withEnv("KAFKA_LISTENER_PORT", "9093")
            .withEnv("KAFKA_ZOOKEEPER_CONNECT", "localhost:2181");
    static {
        if (!kafkaContainer.isRunning())
            kafkaContainer.start();
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