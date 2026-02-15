it stores all offset in meta properties of blob
eventhub-n-prod.servicebus.windows.net/chenile/vymo/checkpoint/0
metadat name is sequencenumber = 65 whatever



Here’s a **technical README** for your Spring-based Azure Event Hub + Blob Storage setup, explaining how it works, how to configure it, and how offsets are managed. I’ve structured it clearly for developers or DevOps to understand and use.

---

# Chenile Event Hub & Blob Storage Integration

## Overview

This project provides a **Spring-based integration** with **Azure Event Hubs** for event streaming and **Azure Blob Storage** for storing offsets/checkpoints. It supports multiple producers and consumers, with a configuration-driven approach.

Key features:

* Supports **AMQP over WebSockets** for Event Hub transport.
* Supports multiple Event Hub topics (hubs) with independent consumers.
* Producers can send events to different hubs asynchronously.
* Consumers can read events and store checkpoint metadata in Blob Storage.
* Checkpoints are stored in blob metadata under a structured path.

---

## Configuration

All configurations are stored under the `spring.chenile` namespace in `application.yml` or `application.properties`.

### Event Hub Configuration

```yaml
spring:
  chenile:
    azure:
      eventhubs:
        transport-type: AMQP_WEB_SOCKETS
        namespace: sb://localhost/;
        connection-string: "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
        auto-start-consumers: false
        consumers:
          hubs:
            chenile:
              consumer-group: "eh1consumer"
            eh2:
              consumer-group: "eh2consumer"
        producers:
          - chenile
          - eh2
```

#### Notes:

* **transport-type**: `AMQP_WEB_SOCKETS` allows communication through firewalls where standard AMQP ports are blocked.
* **namespace**: Event Hub namespace endpoint.
* **connection-string**: Azure Event Hub connection string.
* **auto-start-consumers**: If `true`, consumers start automatically on application startup.
* **consumers.hubs**: Define consumers per Event Hub with their consumer groups.
* **producers**: List of Event Hub topics that can be used to send events.

---

### Blob Storage Configuration

```yaml
spring:
  chenile:
    storage:
      blob:
        endpoint: "http://localhost:10000/devstoreaccount1"
        container: "chenilequeue"
        credential-type: "key"   # options: "key" or "sas"
        account-name: "devstoreaccount1"
        account-key: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
```

#### Notes:

* **endpoint**: Azure Blob Storage endpoint or local emulator URL.
* **container**: The blob container where offsets/checkpoints are stored.
* **credential-type**: Authentication type. Options: `"key"` or `"sas"`.
* **account-name / account-key**: Required if `credential-type` is `"key"`.

---

## How It Works

### Producing Events

1. Create a payload and optional headers.
2. Call `chenilePub.asyncPublish(topic, payload, headers)`.
3. The producer will create an `EventData` object, attach headers, and send it to the configured Event Hub topic.
4. If the topic is not registered in the configuration, an `IllegalStateException` is thrown with a clear message:

```
Azure Event Hub client for topic 'unknown' is not registered. Please add it to the configuration and ensure it is available in the cloud.
```

---

### Consuming Events

1. Consumers are defined per hub in the configuration.
2. Each consumer belongs to a **consumer group**.
3. Consumer reads events from the Event Hub and updates **checkpoint metadata** in Blob Storage.

---

### Checkpoints / Offsets

* Checkpoints are stored as **metadata on blobs** using the following path convention:

```
<namespace>/<hub-name>/<consumer-group>/checkpoint/<partition-id>
```

Example:

```
eventhub-n-prod.servicebus.windows.net/chenile/vymo/checkpoint/0
```

* Each checkpoint blob contains a metadata property:

```text
sequencenumber = 65
```

This ensures that consumers can resume processing from the last read event.

---

## Example Usage

### Publishing an Event

```java
Map<String, Object> headers = new HashMap<>();
headers.put("num3", 10);

Payload payload = new Payload(5, 8);
String payloadJson = new ObjectMapper().writeValueAsString(payload);

chenilePub.asyncPublish("chenile", payloadJson, headers);
```

* Throws `IllegalStateException` if the topic `"chenile"` is not configured.
* The event is sent in a batch to Azure Event Hub.

---

### Unit Testing Unknown Topics

```java
@Test
void testEventHubUnknownTopic() throws JsonProcessingException {
    Payload payload = new Payload(5, 8);
    Map<String, Object> headers = new HashMap<>();
    headers.put("num3", 10);
    String s = new ObjectMapper().writeValueAsString(payload);

    IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> {
        chenilePub.asyncPublish("unknown", s, headers);
    });

    Assertions.assertTrue(exception.getMessage().contains("Azure Event Hub client for topic 'unknown' is not registered"));
}
```

---

### Notes

* Make sure the Event Hub namespace and connection strings are correct.
* Ensure the blob container exists and credentials are valid.
* Consumers only start if `auto-start-consumers` is `true` or manually started.
* Offsets stored in blob metadata guarantee **exactly-once processing** semantics.

---

If you want, I can also create a **diagram showing the flow** of producer → Event Hub → consumer → blob checkpoint. It makes this README much easier to understand visually.

Do you want me to add that diagram?
