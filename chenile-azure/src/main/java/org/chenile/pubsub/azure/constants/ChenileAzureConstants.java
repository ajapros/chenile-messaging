package org.chenile.pubsub.azure.constants;

/** Azure Event Hubs transport property names used by the Chenile Azure adapter. */
public interface ChenileAzureConstants {
    String CHENILE_GLOBAL_TOPIC = "chenile_global_service";
    String CHENILE_TOPIC_KEY = "chenile_topic";

    /** Explicit physical Event Hubs partition ID, for example {@code 0} or {@code 2}. */
    String CHENILE_AZURE_PARTITION_ID = "chenile.azure.partition-id";

    /** Business key that Azure Event Hubs hashes to a stable partition. */
    String CHENILE_AZURE_PARTITION_KEY = "chenile.azure.partition-key";

    /** Per-message routing mode. Set to {@link #CHENILE_AZURE_PARTITION_MODE_AUTO} for Azure distribution. */
    String CHENILE_AZURE_PARTITION_MODE = "chenile.azure.partition-mode";
    String CHENILE_AZURE_PARTITION_MODE_AUTO = "auto";
}
