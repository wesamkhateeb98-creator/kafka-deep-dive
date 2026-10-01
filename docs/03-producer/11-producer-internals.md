# 11 · Producer Internals

> `Produce()` ما بيبعت فوراً: بيحط الـ record بـ **batch** بالـ memory، و thread تاني بيبعت الـ batches.

```mermaid
flowchart LR
    A["Produce(msg)"] --> S["Serializer<br/>key/value → bytes"]
    S --> P["Partitioner<br/>→ partition"]
    P --> ACC["Record Accumulator<br/>batch per partition"]
    ACC -->|"batch full OR linger.ms"| SND["Sender thread"]
    SND -->|"1 request per broker<br/>(many batches)"| B["Leader broker"]
    B -->|ack| CB["delivery report / Task completes"]
```

## Batching triggers
| Config (librdkafka) | Default | يعني |
|---|---|---|
| `linger.ms` | 5 | استنى لحد 5ms لتعبي batch |
| `batch.size` | 1 MB | أكبر batch per partition |
| `batch.num.messages` | 10000 | max records per batch |
| `queue.buffering.max.messages` | 100000 | buffer محلي (full → `Local_QueueFull`) |
| `compression.type` | none | `lz4` / `zstd` على الـ batch كامل |

## Throughput — قياس حقيقي (examples/06, 50K × 512B)
> Windows + Docker Desktop، 3 brokers على نفس الجهاز ⇒ الأرقام المطلقة منخفضة؛ **النسب** هي الدرس.

| Scenario | msgs/s | vs baseline |
|---|---|---|
| acks=0, linger=0 | 13,440 | — |
| acks=1, linger=0 | 12,245 | — |
| **acks=all, linger=0** (baseline) | 4,910 | 1.0x |
| acks=all, linger=20 | 7,171 | 1.5x |
| **acks=all, linger=20, lz4** | **26,727** | **5.4x** |
| acks=all, linger=20, zstd | 14,630 | 3.0x (CPU أكثر، ضغط أعلى) |

→ `acks=all` + batching + lz4 أسرع من `acks=0` بدون batching — **durability ما لازم تكون على حساب السرعة**.

## Async vs Sync
```csharp
// ❌ slow: await كل رسالة = round-trip لكل وحدة
foreach (var m in msgs) await producer.ProduceAsync(topic, m);

// ✅ fast: Produce + callback، flush بالآخر
foreach (var m in msgs) producer.Produce(topic, m, r => { if (r.Error.IsError) Log(r); });
producer.Flush(TimeSpan.FromSeconds(30));

// ✅ fast + async: Task.WhenAll
await Task.WhenAll(msgs.Select(m => producer.ProduceAsync(topic, m)));
```

## Lifecycle
| Rule | ليش |
|---|---|
| **Producer واحد per app** (singleton) | thread-safe، بيحمل connections + buffers |
| `Flush()` قبل الإغلاق | وإلا الـ batches بالـ memory بتضيع |
| لا تعمل producer per request | كل واحد = TCP + threads (~ms لكل إنشاء) |

## Retries
| Config | Default |
|---|---|
| `message.send.max.retries` | ∞ (INT32_MAX) |
| `retry.backoff.ms` | 100 |
| `message.timeout.ms` (= `delivery.timeout.ms`) | 300000 (5 min) |

## Key Points
- Batching = سر الـ throughput
- Compression per batch، مش per record
- Delivery report = مصدر الحقيقة للنجاح

Next → [12-acks-durability.md](12-acks-durability.md)
