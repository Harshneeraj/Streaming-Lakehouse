# Kafka Networking

Kafka's networking is more complex than other services because of one unique behavior: after a client connects, the broker tells the client **"use this address to talk to me going forward."** This forces us to configure multiple listeners.

---

## The four config lines explained

### 1. LISTENERS — "what ports do I open?"

```
KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094"
```

Kafka opens three TCP sockets:

| Name | Port | Purpose |
|------|------|---------|
| PLAINTEXT | 9092 | For containers inside Docker (Spark, producer) |
| CONTROLLER | 9093 | For internal Raft protocol (controller-to-controller) |
| EXTERNAL | 9094 | For clients on the host machine (your laptop) |

Think of it as three doors into the same building.

---

### 2. ADVERTISED_LISTENERS — "what address do I tell clients?"

```
KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094"
```

When a client connects, Kafka responds with: "come back at this address."

| If you connect on... | Kafka tells you to use... | Works for... |
|---------------------|--------------------------|--------------|
| Port 9092 | `kafka:9092` | Docker containers (they can resolve `kafka` via DNS) |
| Port 9094 | `localhost:9094` | Your host machine |

CONTROLLER is not listed here — it's not client-facing.

---

### 3. LISTENER_SECURITY_PROTOCOL_MAP — "what security on each port?"

```
KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT"
```

Maps each listener name to a security protocol:

| Name | Protocol | Meaning |
|------|----------|---------|
| CONTROLLER | PLAINTEXT | No encryption |
| PLAINTEXT | PLAINTEXT | No encryption |
| EXTERNAL | PLAINTEXT | No encryption |

In production you'd use `SSL` or `SASL_SSL` for external traffic.

---

### 4. INTER_BROKER_LISTENER_NAME — "which port for broker-to-broker traffic?"

```
KAFKA_CFG_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
```

In a multi-broker cluster, brokers replicate data between each other. This says: "use the PLAINTEXT listener (port 9092) for that."

In our single-broker setup this is unused but required by Kafka's config validation.

---

## Key point: all listeners share the same data

Both ports (9092 and 9094) connect to the **same broker, same topics, same partitions**. The listener is just the entry point — once inside, it's all the same.

---

## What's actually needed for this project

Since all our clients run inside Docker, only the PLAINTEXT listener is required. The EXTERNAL listener on 9094 is a convenience for debugging from the host.
