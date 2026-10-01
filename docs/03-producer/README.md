# 📤 03 · Producer — الـ Producer

> batching، acks، idempotence، keys و partitioning

```mermaid
flowchart LR
    N0["11 · Producer Internals"]
    N1["12 · acks & Durability"]
    N2["13 · Idempotent Producer"]
    N3["14 · Partitioning & Keys"]
    N0 --> N1 --> N2 --> N3
```

| # | Idea | One line |
|---|---|---|
| 11 | [Producer Internals](11-producer-internals.md) | `Produce()` ما بيبعت فوراً: بيحط الـ record بـ **batch** بالـ memory، و thread تاني بيبعت الـ batches. |
| 12 | [acks & Durability](12-acks-durability.md) | `acks` = كم replica لازم تأكد الكتابة قبل ما الـ producer يعتبرها نجحت. |
| 13 | [Idempotent Producer](13-idempotent-producer.md) | الـ retries ممكن تكتب نفس الرسالة مرتين. **Idempotence** = الـ broker بيكشف التكرار ويرفضه عبر `(ProducerId, Sequence)`. |
| 14 | [Partitioning & Keys](14-partitioning-keys.md) | `partition = hash(key) % numPartitions`. نفس الـ key ⇒ نفس الـ partition ⇒ **ترتيب مضمون لهاد الـ key**. |

← [Replication](../02-replication/README.md) · [Map](../../README.md) · [Consumer](../04-consumer/README.md) →
