package org.chenile.pubsub.kafka;

import static org.junit.Assert.assertThrows;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.chenile.core.model.ChenileConfiguration;
import org.chenile.pubsub.ChenilePub;
import org.chenile.pubsub.kafka.impl.KafkaPublisher;
import org.chenile.pubsub.kafka.configuration.KafkaConfiguration;
import org.chenile.pubsub.provider.PubSubInfoProvider;
import org.junit.Test;
import org.junit.Assert;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import static org.mockito.Mockito.mock;

public class KafkaPublisherWaitTest {

  @Test
  public void zeroWaitRetainsFireAndForgetBehavior() {
    KafkaPublisher publisher =
        publisher(
            CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")),
            0);

    assertNotThrows(() -> publisher.asyncPublish("work", "{}", Map.of()));
  }

  @Test
  public void positiveWaitPropagatesBrokerFailure() {
    KafkaPublisher publisher =
        publisher(
            CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")),
            1_000);

    assertThrows(
        IllegalStateException.class,
        () -> publisher.asyncPublish("work", "{}", Map.of()));
  }

  @Test
  public void positiveWaitReturnsAfterBrokerAcknowledgement() {
    KafkaPublisher publisher =
        publisher(CompletableFuture.completedFuture(null), 1_000);

    assertNotThrows(() -> publisher.asyncPublish("work", "{}", Map.of()));
  }

  @Test
  public void positiveWaitTimesOutIfBrokerDoesNotAcknowledge() {
    KafkaPublisher publisher = publisher(new CompletableFuture<>(), 10);

    assertThrows(
        IllegalStateException.class,
        () -> publisher.asyncPublish("work", "{}", Map.of()));
  }

  @Test
  public void applicationPropertyConfiguresThePublisherWait() {
    new ApplicationContextRunner()
        .withUserConfiguration(KafkaConfiguration.class, TestBeans.class)
        .withPropertyValues("pubsub.publish.waitTime=10")
        .run(
            context -> {
              ChenilePub publisher = context.getBean(ChenilePub.class);

              assertThrows(
                  IllegalStateException.class,
                  () -> publisher.asyncPublish("work", "{}", Map.of()));
            });
  }

  private KafkaPublisher publisher(
      CompletableFuture<SendResult<String, String>> result, long waitTimeMillis) {
    KafkaTemplate<String, String> kafkaTemplate =
        new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of())) {
          @Override
          public CompletableFuture<SendResult<String, String>> send(
              ProducerRecord<String, String> record) {
            return result;
          }
        };
    return new KafkaPublisher(kafkaTemplate, null, waitTimeMillis);
  }

  private void assertNotThrows(Runnable action) {
    try {
      action.run();
    } catch (Throwable failure) {
      Assert.fail("Expected publication to succeed, but got: " + failure);
    }
  }

  @Configuration
  static class TestBeans {
    @Bean
    KafkaTemplate<String, String> kafkaTemplate() {
      return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of())) {
        @Override
        public CompletableFuture<SendResult<String, String>> send(
            ProducerRecord<String, String> record) {
          return new CompletableFuture<>();
        }
      };
    }

    @Bean
    PubSubInfoProvider pubSubInfoProvider() {
      return new PubSubInfoProvider();
    }

    @Bean
    ChenileConfiguration chenileConfiguration() {
      return mock(ChenileConfiguration.class);
    }
  }
}
