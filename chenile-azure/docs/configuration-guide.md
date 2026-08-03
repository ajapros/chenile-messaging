# Chenile Azure Configuration Guide

This document explains the runtime configuration used by the `chenile-azure` module, what each property means, how the module resolves Event Hub names, and which test cases verify the behavior.

## Purpose

This module provides:

- Publishing to Azure Event Hubs through `ChenilePub`
- Subscription from Azure Event Hubs into Chenile event handlers
- Blob Storage based checkpointing for Event Hub consumers
- Optional tenant-specific Event Hub naming through a configurable prefix

The main configuration classes are:

- `chenile.azure.eventhubs.*` bound by [`ChenileEventHubProperties.java`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/main/java/org/chenile/pubsub/azure/configuration/ChenileEventHubProperties.java)
- `chenile.storage.blob.*` bound by [`BlobStorageProperties.java`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/main/java/org/chenile/pubsub/azure/configuration/storage/BlobStorageProperties.java)

## How Resolution Works

When the application publishes a logical topic like `order-created`, the module resolves it in this order:

1. If `routes` contains the topic, use the mapped physical Event Hub name.
2. Otherwise, if `default-route` is set, use that.
3. Otherwise, use the logical topic name as the physical Event Hub name.
4. If `client-prefix-enabled` is `true` and the message tenant is in `clients`, prefix the physical Event Hub name with `<tenant><separator>`.

Examples:

- Logical topic `order-created` with `routes.order-created=business-events` becomes `business-events`
- If prefixing is enabled and tenant is `acme`, the same topic becomes `acme-business-events`
- Logical topic `audit-created` with no explicit route but `default-route=shared-events` becomes `shared-events`

Tenant handling inside the application is separate from Event Hub naming. The subscriber restores tenant context from message headers even when prefixing is disabled.

## Message Interceptors

Applications can register Spring beans that implement `org.chenile.pubsub.interceptor.PubSubMessageInterceptor`.
The interceptor API lives in `chenile-pub-sub`, so the same contract can be reused by other providers.

Azure invokes these hooks:

- `beforePublish(...)` after logical topic routing and before `EventData` is created
- `beforeSubscribe(...)` after Azure body/properties are read and before `EventProcessor.handleEvent(...)`

Example:

```java
@Bean
PubSubMessageInterceptor auditHeaderInterceptor() {
    return new PubSubMessageInterceptor() {
        @Override
        public PubSubMessage beforePublish(PubSubMessage message) {
            message.getHeaders().put("source", "orders");
            return message;
        }

        @Override
        public PubSubMessage beforeSubscribe(PubSubMessage message) {
            message.getHeaders().put("receivedBy", "chenile-azure");
            return message;
        }
    };
}
```

Use Spring `@Order` on interceptor beans when multiple interceptors must run in a fixed order.

## Event Hub Properties

All Event Hub properties are under:

```yaml
chenile:
  azure:
      eventhubs:
```

### `connection-string`

Azure Event Hubs connection string used to build all producer and consumer clients.

Example:

```yaml
chenile:
  azure:
      eventhubs:
        connection-string: "Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=..."
```

Required: Yes

Used by:

- producer client creation
- consumer processor creation

### `producers`

List of logical topics that this application may publish. These values are resolved through `routes` and `default-route`, deduplicated, then used to create producer clients.

Example:

```yaml
producers:
  - chenile
  - order-created
  - invoice-paid
```

Notes:

- This list drives producer client creation at startup.
- If a topic resolves to a physical hub that does not have a registered client, publishing fails fast.

### `routes`

Map of logical topic name to physical Event Hub name.

Example:

```yaml
routes:
  chenile: business-events
  order-created: business-events
  order-updated: business-events
  invoice-paid: billing-events
  eh2: eh2
```

Notes:

- Multiple logical topics may map to the same physical Event Hub.
- Blank route targets are rejected at runtime.

### `default-route`

Fallback physical Event Hub name for logical topics that are not explicitly present in `routes`.

Example:

```yaml
default-route: shared-events
```

Behavior:

- If a topic is not present in `routes`, it resolves to `default-route`
- If `default-route` is blank, resolution fails when first used
- Consumer hubs listed explicitly in `consumers.hubs` are not automatically rewritten to `default-route`

### `consumers.hubs.<hub-name>.consumer-group`

Defines which physical or logical hubs should have consumers, and which consumer group each one uses.

Example:

```yaml
consumers:
  hubs:
    business-events:
      consumer-group: eh1consumer
    billing-events:
      consumer-group: eh1consumer
    shared-events:
      consumer-group: eh1consumer
    eh2:
      consumer-group: eh2consumer
```

Notes:

- Consumer hubs are resolved through `routes` only when the configured key itself appears in `routes`
- If two configured hubs collapse to the same physical hub but use different consumer groups, startup fails

### `auto-start-consumers`

Controls whether all Event Hub consumer processors are started automatically at `ApplicationReadyEvent`.

Example:

```yaml
auto-start-consumers: true
```

Behavior:

- `true`: consumers start automatically
- `false`: application must call `EventHubConsumerStarter.startConsumersManually()`

## Per-Message Partition Routing Matrix

Partition routing is supplied in the `Map<String, Object>` passed to
`chenilePub.asyncPublish(...)`. It is not an application YAML setting.

| Message properties | Azure Event Hubs behavior | Use when |
| --- | --- | --- |
| No routing properties | Sends to partition `0` | Backward-compatible/default processing |
| `chenile.azure.partition-id: 2` | Sends directly to physical partition `2` | The producer deliberately owns partition selection |
| `chenile.azure.partition-key: customer-123` | Azure hashes the key and keeps that key on one stable partition | Per-customer, loan, or aggregate ordering is required |
| `chenile.azure.partition-mode: auto` | No selector is sent; Azure distributes independent messages across partitions | Throughput matters more than per-key ordering |

Only one routing strategy may be selected for an event. These combinations fail before the
message is sent:

- `partition-id` with `partition-key`
- `partition-mode: auto` with either `partition-id` or `partition-key`
- any partition mode other than `auto`

An invalid explicit `partition-id` falls back to partition `0`, preserving the prior publisher
behavior. With three Event Hub partitions, three replicas in the same consumer group can process
one assigned partition each; Azure rebalances partition ownership when replicas change.

Example:

```java
// Azure-hashed routing: all events for this customer retain partition order.
chenilePub.asyncPublish("order-created", payload,
        Map.of("chenile.azure.partition-key", customerId));

// Opt in to automatic distribution for independent events.
chenilePub.asyncPublish("telemetry", payload,
        Map.of("chenile.azure.partition-mode", "auto"));
```

### `dl`

Dead-letter logical topic or physical Event Hub name. When an event handler returns an exchange with an exception, the subscriber republishes the original payload to this destination.

Example:

```yaml
dl: eh2
```

Notes:

- This value also participates in producer client creation
- It is resolved through `routes` and optionally through client prefixing

### `clients`

List of tenant or client identifiers that are allowed to participate in Event Hub name prefixing.

Example:

```yaml
clients:
  - acme
  - beta
```

Notes:

- `clients` by itself does nothing
- Prefixing only happens when `client-prefix-enabled` is `true`

### `client-prefix-enabled`

Controls whether tenant IDs are used to derive tenant-specific physical Event Hub names.

Example:

```yaml
client-prefix-enabled: true
```

Behavior:

- `false` or omitted: producer and consumer names stay unprefixed
- `true`: if the tenant header matches a configured value in `clients`, the physical Event Hub becomes `<tenant><separator><hub>`

Examples:

- tenant `acme`, physical hub `business-events`, separator `-` becomes `acme-business-events`
- tenant `unknown` remains `business-events`

This is the correct choice when:

- you have separate Event Hubs per tenant

It should remain disabled when:

- you use shared Event Hubs and keep tenant separation only in headers and application logic

### `client-prefix-separator`

Separator used between tenant ID and physical Event Hub name.

Example:

```yaml
client-prefix-separator: "-"
```

If blank, the code falls back to `_`.

## Blob Storage Properties

All Blob Storage properties are under:

```yaml
chenile:
  storage:
      blob:
```

These are used to create the `BlobContainerAsyncClient` backing Azure's `BlobCheckpointStore`.

### `endpoint`

Blob service endpoint.

Example:

```yaml
endpoint: "http://localhost:10000/devstoreaccount1"
```

### `container`

Blob container used for checkpoint blobs.

Example:

```yaml
container: "chenilequeue"
```

### `credential-type`

Authentication type for Blob Storage. Supported values:

- `key`
- `sas`

Examples:

```yaml
credential-type: "key"
```

```yaml
credential-type: "sas"
```

If another value is used, startup fails with `Unsupported blob credential type`.

### `account-name`

Required when `credential-type: key`.

### `account-key`

Required when `credential-type: key`.

### `sas-token`

Required when `credential-type: sas`.

## Example Configurations

### Example 1: Shared Event Hubs, no tenant prefix

Use this when all tenants share the same physical Event Hubs and tenant separation is handled in message headers and Chenile context.

```yaml
chenile:
  azure:
      eventhubs:
        connection-string: "Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=..."
        auto-start-consumers: true
        producers:
          - order-created
          - invoice-paid
          - audit-created
        routes:
          order-created: business-events
          invoice-paid: billing-events
        default-route: shared-events
        consumers:
          hubs:
            business-events:
              consumer-group: eh1consumer
            billing-events:
              consumer-group: eh1consumer
            shared-events:
              consumer-group: eh1consumer
        dl: error-events
    storage:
      blob:
        endpoint: "https://storage-account.blob.core.windows.net"
        container: "chenilequeue"
        credential-type: "key"
        account-name: "storage-account"
        account-key: "..."
```

Resolution examples:

- `order-created` -> `business-events`
- `invoice-paid` -> `billing-events`
- `audit-created` -> `shared-events`

### Example 2: Tenant-specific Event Hubs with prefixing

Use this when each tenant has its own physical Event Hubs.

```yaml
chenile:
  azure:
      eventhubs:
        connection-string: "Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=..."
        auto-start-consumers: false
        producers:
          - chenile
          - order-created
          - invoice-paid
        routes:
          chenile: business-events
          order-created: business-events
          invoice-paid: billing-events
          eh2: eh2
        default-route: shared-events
        clients:
          - acme
          - beta
        client-prefix-enabled: true
        client-prefix-separator: "-"
        consumers:
          hubs:
            business-events:
              consumer-group: eh1consumer
            billing-events:
              consumer-group: eh1consumer
            shared-events:
              consumer-group: eh1consumer
            eh2:
              consumer-group: eh2consumer
        dl: eh2
    storage:
      blob:
        endpoint: "http://localhost:10000/devstoreaccount1"
        container: "chenilequeue"
        credential-type: "key"
        account-name: "devstoreaccount1"
        account-key: "..."
```

Resolution examples:

- topic `order-created`, tenant `acme` -> `acme-business-events`
- topic `invoice-paid`, tenant `beta` -> `beta-billing-events`
- topic `audit-created`, tenant `acme` -> `acme-shared-events`
- dead-letter `eh2`, tenant `beta` -> `beta-eh2`

### Example 3: Blob SAS credentials

```yaml
chenile:
  storage:
      blob:
        endpoint: "https://storage-account.blob.core.windows.net"
        container: "chenilequeue"
        credential-type: "sas"
        sas-token: "sv=...&ss=...&srt=...&sp=..."
```

## Configuration Notes and Caveats

### `transport-type` and `namespace`

The sample test YAML contains:

```yaml
transport-type: AMQP_WEB_SOCKETS
namespace: sb://localhost/;
```

These values are not currently bound in [`ChenileEventHubProperties.java`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/main/java/org/chenile/pubsub/azure/configuration/ChenileEventHubProperties.java) and are not used when producer or consumer clients are created. The current code uses only `connection-string` and the resolved Event Hub name.

So as of now:

- `connection-string` is effective
- `transport-type` is informational only in the sample
- `namespace` is informational only in the sample

### Checkpointing

Checkpoint storage is handled by Azure's `BlobCheckpointStore`, created in [`AzureStorageChenileConfiguration.java`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/main/java/org/chenile/pubsub/azure/configuration/storage/AzureStorageChenileConfiguration.java). This module does not implement custom checkpoint naming logic itself.

### Startup Behavior

When `auto-start-consumers` is `true`, [`EventHubConsumerStarter.java`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/main/java/org/chenile/pubsub/azure/configuration/EventHubConsumerStarter.java) starts all `EventProcessorClient` instances on application ready. If startup of any processor fails, the application fails fast.

### Consumer Deduplication

If multiple logical topics route to the same physical Event Hub and use the same consumer group, the module deduplicates them correctly. If consumer groups conflict for the same physical hub, it throws an exception.

## Test Cases

The current test suite already covers most configuration behavior.

### Routing and fallback

- [`ChenileEventHubPropertiesTest.java`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/test/java/org/chenile/pubsub/azure/configuration/ChenileEventHubPropertiesTest.java)
  - distinct producer hub resolution from multiple logical topics
  - consumer hub deduplication after route resolution
  - rejection of conflicting consumer groups
  - fallback to logical topic when no route exists
  - use of `default-route`
  - rejection of blank route targets
  - rejection of blank `default-route`

### Spring property binding

- [`ChenileEventHubPropertiesBindingTest.java`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/test/java/org/chenile/pubsub/azure/configuration/ChenileEventHubPropertiesBindingTest.java)
  - YAML binding of `routes`
  - YAML binding of `default-route`
  - default value of `client-prefix-enabled`

### Producer behavior

- [`AzurePublisherTest.java`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/test/java/org/chenile/pubsub/azure/pub/AzurePublisherTest.java)
  - header building with null and non-null properties
  - route resolution during publish
  - tenant prefix application when enabled
  - no tenant prefix when disabled
  - failure on unknown physical hub
  - failure on blank route target
  - use of `default-route`

### Subscriber and dead-letter behavior

- [`AzureEventHubSubscriberTest.java`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/test/java/org/chenile/pubsub/azure/sub/AzureEventHubSubscriberTest.java)
  - dead-letter publish on handler exception
  - propagation of dead-letter publish failures

- [`AzureEventHubSubscriberTenantTest.java`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/test/java/org/chenile/pubsub/azure/sub/AzureEventHubSubscriberTenantTest.java)
  - tenant context set from headers
  - previous tenant restored after processing
  - no tenant leakage when header is missing

### Prefix expansion utility

- [`EventHubNameUtilsTest.java`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/test/java/org/chenile/pubsub/azure/EventHubNameUtilsTest.java)
  - prefixing with matching tenant
  - no prefix with unknown tenant
  - no prefix when clients are absent
  - expansion of producer and consumer hub names

### End-to-end integration

- [`TestEventHubIntegration.java`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/test/java/org/chenile/pubsub/azure/TestEventHubIntegration.java)
  - prefixed producer and consumer flows using the emulator
  - explicit route handling
  - default-route handling
  - dead-letter flow
  - tenant-specific processing observations

## Suggested Additional Test Cases

These are not required for runtime correctness today, but they would improve coverage:

- Blob configuration failure when `credential-type` is invalid
- Blob configuration success path using SAS credentials
- Consumer startup behavior with `auto-start-consumers=true`
- End-to-end integration without tenant prefixing
- Behavior when tenant header exists but is not listed in `clients`

## Current Test Fixture Example

The repo's integration-style test fixture is in:

- [`application.yml`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/test/resources/application.yml)
- [`Config.json`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/test/resources/Config.json)
- [`docker-compose.yml`](/Users/gauravbhardwaj/work/ajapro/chenile-mqtt/chenile-azure/src/test/resources/docker-compose.yml)

That fixture models tenant-specific Event Hubs such as:

- `acme-business-events`
- `beta-business-events`
- `acme-billing-events`
- `beta-billing-events`
- `acme-shared-events`
- `beta-shared-events`
- `acme-eh2`
- `beta-eh2`

This is why the test YAML enables:

```yaml
client-prefix-enabled: true
client-prefix-separator: "-"
clients:
  - acme
  - beta
```

## Summary

The most important configuration choices are:

- whether topics are routed with `routes`
- whether unmapped topics should go to `default-route`
- whether tenants share hubs or use tenant-prefixed hubs
- whether consumers start automatically
- how Blob Storage credentials are supplied for checkpointing

For shared hubs, keep `client-prefix-enabled` off.

For tenant-specific hubs, turn `client-prefix-enabled` on and define `clients`.
