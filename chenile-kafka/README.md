# Chenile Kafka

`KafkaPublisher` implements Chenile's `ChenilePub` contract over Kafka. By default, publishing
does not wait for the broker acknowledgement. Set `pubsub.publish.waitTime` in the application
configuration to a positive number of milliseconds such as `30000` to wait for the acknowledgement
and propagate send failures to the caller. This is useful when an outbox command must remain
retryable until Kafka acknowledges publication. The default is `0`, which preserves fire-and-forget
behavior.

`CustomKafkaConsumer` subscribes to the Chenile events configured in the application. By default,
each event ID is also its Kafka topic. Applications can choose the consumed topics and map them to
Chenile event IDs with `chenile.kafka.consumer.topic-bindings`, where each map key is a Kafka topic
and its value is the Chenile event ID. For example, `process-work: process-work` binds the
`process-work` topic to the `process-work` Chenile event. Several topics may target the same event.

Subscriber failures and missing subscribers are propagated to Spring Kafka. The consumer retries
using a fixed delay and bounded retry count, then publishes the original record to a dead-letter
topic formed by appending `.DLT` to its source topic. Configure these values with
`chenile.kafka.consumer.retry-interval-ms`, `chenile.kafka.consumer.max-retries`, and
`chenile.kafka.consumer.dead-letter-suffix`. The default is a 1 second delay, 3 retries after the
first delivery, and a `.DLT` suffix. Disable Kafka auto-commit so failed records are not committed
before retry or dead-letter recovery completes. Provision the resulting dead-letter topics in
Kafka so exhausted records can be preserved.
