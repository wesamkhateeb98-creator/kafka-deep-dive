# 04 · KRaft (and goodbye ZooKeeper)

> الـ **metadata** (topics, partitions, leaders, configs, ACLs) صارت بـ Kafka نفسها عبر Raft. **Kafka 4.0 شالت ZooKeeper نهائياً.**

## قبل vs بعد
```mermaid
flowchart LR
    subgraph zkmode["≤ 3.x: ZooKeeper mode"]
        ZK[("ZooKeeper<br/>ensemble")] --- CT["1 Controller broker"]
        CT --- OB1["Broker"] & OB2["Broker"]
    end
    subgraph kraftmode["4.x: KRaft"]
        Q["Controller quorum<br/>(Raft, 3 nodes)<br/>__cluster_metadata log"] --> NB1["Broker"] & NB2["Broker"] & NB3["Broker"]
    end
```

## كيف بيشتغل
```mermaid
sequenceDiagram
    participant C1 as Controller 1 (active leader)
    participant C2 as Controller 2
    participant C3 as Controller 3
    participant B as Broker
    C1->>C2: append metadata record (topic created)
    C1->>C3: append metadata record
    C2-->>C1: ack
    Note over C1: majority (2/3) = committed
    B->>C1: fetch metadata log (like a consumer)
    B->>C1: heartbeat (every 2s)
```

## ZooKeeper vs KRaft
| | ZooKeeper | KRaft |
|---|---|---|
| Systems to run | 2 (Kafka + ZK) | 1 |
| Controller failover | ثواني → دقائق (reload كل metadata) | **ms** (hot standby) |
| Max partitions | ~200K | **millions** |
| Metadata storage | znodes | log `__cluster_metadata` |
| Status | ❌ removed in 4.0 | ✅ only mode |

## Roles
```yaml
KAFKA_PROCESS_ROLES: "broker,controller"   # combined (dev) — مثل docker-compose هون
KAFKA_PROCESS_ROLES: "controller"          # dedicated (prod)
KAFKA_PROCESS_ROLES: "broker"
KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
```

## Quorum math
| Controllers | Tolerates failures |
|---|---|
| 1 | 0 |
| 3 | 1 |
| 5 | 2 |

## جرّب
```bash
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:19092 describe --status
dotnet run --project examples/dotnet/01-Admin    # prints the active controller
```

## Key Points
- Controller = مدير الـ metadata، مش الداتا
- Odd number: 3 أو 5
- Prod: controllers منفصلين عن brokers

Next → [05-topics.md](05-topics.md)
