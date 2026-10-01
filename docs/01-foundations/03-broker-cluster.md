# 03 · Broker & Cluster

> **Broker** = سيرفر Kafka واحد (process). **Cluster** = مجموعة brokers بتتقاسم الـ partitions.

```mermaid
flowchart TB
    subgraph cluster["Kafka Cluster"]
        B1["Broker 1<br/>orders-p0 (L)<br/>orders-p1 (F)"]
        B2["Broker 2<br/>orders-p1 (L)<br/>orders-p2 (F)"]
        B3["Broker 3<br/>orders-p2 (L)<br/>orders-p0 (F)"]
    end
    C["Client"] -->|"1. bootstrap: localhost:29092"| B1
    B1 -->|"2. metadata: p0→B1, p1→B2, p2→B3"| C
    C -->|"3. produce p1 direct"| B2
```

## Bootstrap flow
```mermaid
sequenceDiagram
    participant App
    participant B1 as Broker 1 (any)
    participant B2 as Broker 2 (leader p1)
    App->>B1: MetadataRequest
    B1-->>App: brokers + leaders of every partition
    App->>B2: ProduceRequest(p1)
    Note over App: metadata cached, refreshed every 5 min<br/>or on NOT_LEADER error
```

## Example
```csharp
// أي broker بيكفي — بس حط 3 لو واحد واقع
BootstrapServers = "localhost:29092,localhost:29093,localhost:29094"
```

## Listeners (سبب مشاكل الـ Docker الأشهر)
| Config | يعني |
|---|---|
| `listeners` | وين الـ broker **يسمع** (bind) |
| `advertised.listeners` | العنوان اللي **يعطيه للـ clients** بالـ metadata |

```yaml
KAFKA_LISTENERS:            INTERNAL://:19092,EXTERNAL://:29092
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:19092,EXTERNAL://localhost:29092
```

## Numbers
| | Typical |
|---|---|
| Partitions / broker | ≤ 4000 (recommended) |
| Partitions / cluster (KRaft) | millions ممكن |
| Min prod brokers | 3 |

## Key Points
- Client بيحكي مع الـ **leader** مباشرة
- ما في "master broker" للداتا
- Bootstrap = بس أول اتصال

## Pitfall
❌ `advertised.listeners=localhost` بـ Docker والـ app بـ container تاني → `Connection refused` → ✅ listener داخلي باسم الـ container + خارجي لـ localhost.

Next → [04-kraft.md](04-kraft.md)
