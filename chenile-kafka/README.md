# Chenile Kafka

`KafkaPublisher` implements Chenile's `ChenilePub` contract over Kafka. By default, publishing
does not wait for the broker acknowledgement. Set `pubsub.publish.waitTime` in the application
configuration to a positive number of milliseconds such as `30000` to wait for the acknowledgement
and propagate send failures to the caller. This is useful when an outbox command must remain
retryable until Kafka acknowledges publication. The default is `0`, which preserves fire-and-forget
behavior.
