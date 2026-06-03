package org.chenile.pubsub.logging;

import org.chenile.core.external.ExternalApiLogPublisher;
import org.chenile.core.external.ExternalApiProperties;
import org.chenile.pubsub.ChenilePub;
import org.springframework.beans.factory.ObjectProvider;

/**
 * @deprecated Use {@link PubSubExternalApiPublisher}.
 */
@Deprecated
public class PubSubExternalApiLogPublisher extends PubSubExternalApiPublisher implements ExternalApiLogPublisher {
    public PubSubExternalApiLogPublisher(ObjectProvider<ChenilePub> chenilePubProvider,
                                         ExternalApiProperties properties) {
        super(chenilePubProvider, properties);
    }
}
