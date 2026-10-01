# 💾 06 · Storage — التخزين

> retention، compaction، segments، ليش Kafka سريعة

```mermaid
flowchart LR
    N0["21 · Retention & Log Compaction"]
    N1["22 · Storage Internals: ليش Kafka سريعة؟"]
    N0 --> N1
```

| # | Idea | One line |
|---|---|---|
| 21 | [Retention & Log Compaction](21-retention-compaction.md) | Kafka بتحذف بطريقتين: **delete** (حسب الزمن/الحجم) أو **compact** (خلي آخر value لكل key). |
| 22 | [Storage Internals: ليش Kafka سريعة؟](22-storage-internals.md) | partition = folder فيه **segments**. كل segment = ملف `.log` + indexes. السرعة من: sequential I/O + page cache + zero-copy + batching. |

← [Delivery Semantics](../05-delivery-semantics/README.md) · [Map](../../README.md) · [Integration Patterns](../07-integration-patterns/README.md) →
