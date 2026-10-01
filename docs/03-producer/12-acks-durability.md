# 12 · acks & Durability

> `acks` = كم replica لازم تأكد الكتابة قبل ما الـ producer يعتبرها نجحت.

```mermaid
sequenceDiagram
    participant P as Producer
    participant L as Leader
    participant F as Followers (ISR)
    rect rgb(255, 235, 235)
    Note over P,L: acks=0
    P->>L: send
    Note over P: done (no wait)
    end
    rect rgb(255, 250, 225)
    Note over P,L: acks=1
    P->>L: send
    L-->>P: ack after local write
    end
    rect rgb(230, 250, 230)
    Note over P,F: acks=all (-1)
    P->>L: send
    F->>L: fetch
    L-->>P: ack after ALL ISR have it
    end
```

## المقارنة
| acks | Latency | Loss scenario | Use |
|---|---|---|---|
| `0` | ~0 | الـ broker ما استلم ولا تدري | metrics/logs غير مهمة |
| `1` | 1 write | leader استلم ومات قبل ما ينسخ | logs |
| `all` | ISR round-trip | فقط لو كل الـ ISR ماتوا | **orders, payments — default** |

## Loss scenario مع acks=1
```mermaid
flowchart LR
    A["leader writes offset 100<br/>acks producer ✅"] --> B["leader dies 💥<br/>before follower fetched"]
    B --> C["follower becomes leader<br/>(LEO = 100)"]
    C --> D["offset 100 is gone ❌<br/>producer thinks it succeeded"]
```

## Durability recipe
```csharp
var config = new ProducerConfig
{
    Acks = Acks.All,
    EnableIdempotence = true,
};
// topic: replication.factor=3, min.insync.replicas=2
// broker: unclean.leader.election.enable=false
```

## Defaults
| Client | acks default | idempotence default |
|---|---|---|
| Java ≥ 3.0 | `all` | `true` |
| librdkafka / .NET | `all` | **`false`** ← فعّلها يدوياً |

## Key Points
- acks=all بدون min.isr=2 = ضعيف
- "ack" = replicated بالـ memory/page cache، مش fsync
- Kafka تعتمد على replication مش fsync

Next → [13-idempotent-producer.md](13-idempotent-producer.md)
