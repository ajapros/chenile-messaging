package org.chenile.pubsub;

import java.util.Map;

/**
 * A transport-neutral consumer for a logical Chenile pub/sub topic.
 *
 * <p>This is deliberately smaller than a service controller. It is useful for
 * infrastructure messages, such as process-manager commands, where an
 * application needs a role-specific consumer without exposing an HTTP service.
 * Implementations must be idempotent: a broker can deliver a message again
 * before its checkpoint is committed.</p>
 */
public interface ChenileMessageHandler {

    /** True when this handler owns the supplied logical topic. */
    boolean supports(String topic);

    /** Handle one message. Throwing prevents the transport from checkpointing it. */
    void handle(String topic, String payload, Map<String, String> headers);
}
